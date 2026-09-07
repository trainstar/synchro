# R3.1 final reference deletion-contract table

Count: **603 total rows**. Targets: `mutation-conservation` 72, `cursor-monotonicity` 89, `checksum-convergence` 31, `scope-isolation` 83, `no-state-forks` 50, `oracle-internal` 161, `ENGINE-GAP` 114, `WITHDRAWN` 3

The review target counts total 603. This table has three `WITHDRAWN` rows because the current source lacks the checks described by REF-556 through REF-558. Therefore, the review categories reconcile to 600 active target rows, plus 3 withdrawn rows.

## Full merged table

| check-id | source | verified behavior | target | negative control |
|---|---|---|---|---|
| REF-001 | conformance/reference/client.go:108 | Rejects recovery requests without an action. | ENGINE-GAP | Mutant accepts a missing recovery action. |
| REF-002 | conformance/reference/client.go:111 | Rejects recovery actions outside retry and remediated. | ENGINE-GAP | Mutant accepts an unknown recovery action. |
| REF-003 | conformance/reference/client.go:116 | Requires a durable error state before recovery. | ENGINE-GAP | Mutant recovers a client without an error state. |
| REF-004 | conformance/reference/client.go:119 | Requires remediation before retrying a non-retryable error. | ENGINE-GAP | Mutant permits retry of a terminal error. |
| REF-005 | conformance/reference/client.go:147 | Rejects restart from an unknown lifecycle. | ENGINE-GAP | Mutant treats an unknown lifecycle as restartable. |
| REF-006 | conformance/reference/client.go:181 | Requires an existing client before retirement. | ENGINE-GAP | Mutant creates a retirement record for an unknown client. |
| REF-007 | conformance/reference/client.go:184 | Rejects retirement of an already retired client. | ENGINE-GAP | Mutant overwrites an existing retirement record. |
| REF-008 | conformance/reference/client.go:206 | Requires a nonempty user identity. | ENGINE-GAP | Mutant accepts an empty user identity. |
| REF-009 | conformance/reference/client.go:209 | Requires a nonempty client identity. | ENGINE-GAP | Mutant accepts an empty client identity. |
| REF-010 | conformance/reference/client.go:304 | Rejects an unknown prior lifecycle during transition. | ENGINE-GAP | Mutant permits an unknown prior lifecycle. |
| REF-011 | conformance/reference/client.go:307 | Rejects an unknown next lifecycle during transition. | ENGINE-GAP | Mutant permits an unknown next lifecycle. |
| REF-012 | conformance/reference/client.go:310 | Rejects lifecycle edges outside the transition graph. | ENGINE-GAP | Mutant permits a forbidden lifecycle edge. |
| REF-013 | conformance/reference/client.go:350 | Rejects client-generation allocation at the protocol counter limit. | cursor-monotonicity | Mutant allocates a generation after the limit. |
| REF-014 | conformance/reference/client.go:361 | Requires the current client generation in generation history. | cursor-monotonicity | Mutant expires a generation absent from history. |
| REF-015 | conformance/reference/client.go:368 | Requires a positive stale-client interval. | ENGINE-GAP | Mutant accepts a zero stale interval. |
| REF-016 | conformance/reference/client.go:375 | Requires activity time for stale-client evaluation. | ENGINE-GAP | Mutant treats a generation without activity as active. |
| REF-017 | conformance/reference/client.go:379 | Rejects stale intervals outside the duration range. | ENGINE-GAP | Mutant overflows stale-client duration conversion. |
| REF-018 | conformance/reference/client.go:400 | Requires the current generation in history during expiry. | cursor-monotonicity | Mutant expires a generation through a missing history entry. |
| REF-019 | conformance/reference/client.go:502 | Requires local state for cursor installation. | cursor-monotonicity | Mutant installs a cursor into nil local state. |
| REF-020 | conformance/reference/client.go:506 | Requires an existing local checkpoint for cursor replacement. | cursor-monotonicity | Mutant creates a replacement checkpoint silently. |
| REF-021 | conformance/reference/client_operations_test.go:44 | Fresh connect returns a typed HTTP connect result. | ENGINE-GAP | Mutant returns an untyped fresh-connect result. |
| REF-022 | conformance/reference/client_operations_test.go:47 | Fresh connect selects schema replacement. | ENGINE-GAP | Mutant reports no schema action for fresh connect. |
| REF-023 | conformance/reference/client_operations_test.go:50 | Fresh connect issues generation one with authoritative assignment versions. | cursor-monotonicity | Mutant issues an incorrect initial generation or assignment lineage. |
| REF-024 | conformance/reference/client_operations_test.go:55 | Fresh connect durably installs schema, generation, and null cursor state. | no-state-forks | Mutant reports connect success without durable local state. |
| REF-025 | conformance/reference/client_operations_test.go:59 | Fresh connect does not acknowledge a cursor. | cursor-monotonicity | Mutant acknowledges a cursor during registration. |
| REF-026 | conformance/reference/client_operations_test.go:73 | Current-schema connect selects no schema action. | ENGINE-GAP | Mutant reports replacement for an unchanged schema. |
| REF-027 | conformance/reference/client_operations_test.go:76 | Current-schema connect reports no affected scopes. | scope-isolation | Mutant reports unrelated scopes as affected. |
| REF-028 | conformance/reference/client_operations_test.go:80 | Current-schema connect preserves an acknowledged server checkpoint. | cursor-monotonicity | Mutant rewinds or replaces an acknowledged checkpoint. |
| REF-029 | conformance/reference/client_operations_test.go:92 | Class 2 connect selects schema replacement. | ENGINE-GAP | Mutant selects a rebuild for a compatible class 2 change. |
| REF-030 | conformance/reference/client_operations_test.go:96 | Class 2 connect preserves typed migration metadata. | ENGINE-GAP | Mutant drops or changes migration metadata. |
| REF-031 | conformance/reference/client_operations_test.go:109 | Class 3 connect reports exactly the affected scopes. | scope-isolation | Mutant includes an unaffected scope in class 3 dispatch. |
| REF-032 | conformance/reference/client_operations_test.go:113 | Class 3 connect enters scope-local rebuild state. | scope-isolation | Mutant skips rebuild for an affected scope. |
| REF-033 | conformance/reference/client_operations_test.go:126 | Class 3 connect leaves unaffected scopes out of dispatch. | scope-isolation | Mutant rebuilds an unaffected scope. |
| REF-034 | conformance/reference/client_operations_test.go:138 | Class 4 connect returns the typed unsupported result. | ENGINE-GAP | Mutant reports class 4 as a successful replacement. |
| REF-035 | conformance/reference/client_operations_test.go:142 | Class 4 connect enters the typed upgrade error state. | ENGINE-GAP | Mutant omits the terminal schema error state. |
| REF-036 | conformance/reference/client_operations_test.go:166 | Schema reset reports target schema and affected assignments. | scope-isolation | Mutant resets schema without marking assigned scopes. |
| REF-037 | conformance/reference/client_operations_test.go:170 | Schema reset installs target schema and rebuild markers. | no-state-forks | Mutant reports reset success without durable rebuild state. |
| REF-038 | conformance/reference/client_operations_test.go:173 | Schema reset preserves durable intent and local-only rows. | mutation-conservation | Mutant clears pending intent or local-only data during reset. |
| REF-039 | conformance/reference/client_operations_test.go:193 | Generation expires from creation time before its first acknowledgement. | cursor-monotonicity | Mutant requires an acknowledgement before expiry. |
| REF-040 | conformance/reference/client_operations_test.go:209 | A newer acknowledgement extends generation activity. | cursor-monotonicity | Mutant expires from the older creation time. |
| REF-041 | conformance/reference/client_operations_test.go:232 | Renewal allocates generation two. | cursor-monotonicity | Mutant reuses or skips the next generation. |
| REF-042 | conformance/reference/client_operations_test.go:236 | Generation renewal marks assigned scopes for rebuild. | scope-isolation | Mutant renews without rebuilding assigned scopes. |
| REF-043 | conformance/reference/client_operations_test.go:239 | Generation renewal preserves durable client records. | no-state-forks | Mutant drops durable records during renewal. |
| REF-044 | conformance/reference/client_operations_test.go:242 | Renewal retains an old sealed batch as abandoned history. | mutation-conservation | Mutant deletes or reuses the old sealed batch. |
| REF-045 | conformance/reference/client_operations_test.go:253 | Retired clients fail connect with the retirement gate. | ENGINE-GAP | Mutant permits a retired client to connect. |
| REF-046 | conformance/reference/client_operations_test.go:269 | Protocol-version mismatch returns upgrade-required. | ENGINE-GAP | Mutant accepts an unsupported protocol version. |
| REF-047 | conformance/reference/client_operations_test.go:279 | Runtime below the configured minimum returns upgrade-required. | ENGINE-GAP | Mutant accepts an obsolete runtime. |
| REF-048 | conformance/reference/client_operations_test.go:289 | Retired clients fail the retirement gate. | ENGINE-GAP | Mutant returns a normal connect result for retirement. |
| REF-049 | conformance/reference/client_operations_test.go:298 | Fresh sentinel connect rejects durable prior state. | no-state-forks | Mutant treats stale durable state as a fresh client. |
| REF-050 | conformance/reference/client_operations_test.go:316 | Assignment changes advance the scope-set version correctly. | scope-isolation | Mutant keeps the old scope-set version after assignment change. |
| REF-051 | conformance/reference/client_operations_test.go:320 | Assignment changes report added and removed scopes. | scope-isolation | Mutant reports an incorrect assignment delta. |
| REF-052 | conformance/reference/client_operations_test.go:324 | Removed scopes leave local assignment state. | scope-isolation | Mutant retains a removed local scope. |
| REF-053 | conformance/reference/client_operations_test.go:327 | Assignment changes preserve the queue and mark new scopes for rebuild. | mutation-conservation | Mutant clears the queue or leaves a new scope ready. |
| REF-054 | conformance/reference/client_operations_test.go:342 | Published schema reports the expected lineage action. | ENGINE-GAP | Mutant emits the wrong schema transition action. |
| REF-055 | conformance/reference/client_operations_test.go:346 | Published schema does not overwrite an immutable record. | no-state-forks | Mutant replaces an existing schema manifest. |
| REF-056 | conformance/reference/client_operations_test.go:349 | Rejected schema publication leaves state unchanged. | no-state-forks | Mutant mutates schema state before rejecting publication. |
| REF-057 | conformance/reference/client_operations_test.go:363 | Stop transitions the lifecycle to stopped. | ENGINE-GAP | Mutant leaves the client ready after stop. |
| REF-058 | conformance/reference/client_operations_test.go:367 | Restart after stop transitions to local-ready. | ENGINE-GAP | Mutant restarts directly into a later lifecycle. |
| REF-059 | conformance/reference/client_operations_test.go:383 | Restart retains backoff and queue in local-ready. | mutation-conservation | Mutant drops retry or queued mutation state on restart. |
| REF-060 | conformance/reference/client_operations_test.go:394 | Retry recovery rejects a non-retryable error. | ENGINE-GAP | Mutant retries a terminal local error. |
| REF-061 | conformance/reference/client_operations_test.go:397 | Illegal recovery leaves durable state unchanged. | no-state-forks | Mutant mutates error state before rejecting recovery. |
| REF-062 | conformance/reference/client_operations_test.go:401 | Explicit remediation acknowledges and recovers the error. | no-state-forks | Mutant leaves the error unacknowledged after remediation. |
| REF-063 | conformance/reference/client_operations_test.go:421 | Lifecycle transition table matches every representative edge. | ENGINE-GAP | Mutant changes an allowed or forbidden lifecycle edge. |
| REF-064 | conformance/reference/client_operations_test.go:426 | A representative illegal lifecycle edge is rejected. | ENGINE-GAP | Mutant accepts an illegal lifecycle transition. |
| REF-065 | conformance/reference/client_operations_test.go:447 | Scope compaction preserves floor-equal cursors and rejects older cursors. | cursor-monotonicity | Mutant compacts past a cursor floor. |
| REF-066 | conformance/reference/client_operations_test.go:461 | High-watermark fallback removes all compactable effects. | cursor-monotonicity | Mutant leaves effects at or below the fallback floor. |
| REF-067 | conformance/reference/client_operations_test.go:474 | Bounded compaction limits deletion to the requested batch. | mutation-conservation | Mutant deletes more effects than the compaction bound. |
| REF-068 | conformance/reference/client_operations_test.go:494 | Compaction rejects a batch above the configured maximum. | ENGINE-GAP | Mutant accepts an oversized compaction batch. |
| REF-069 | conformance/reference/client_operations_test.go:498 | Invalid compaction size returns the typed invalid-limit error. | ENGINE-GAP | Mutant returns a generic or successful result. |
| REF-070 | conformance/reference/client_operations_test.go:501 | Invalid compaction size leaves state unchanged. | no-state-forks | Mutant changes floor or effects before rejecting size. |
| REF-071 | conformance/reference/client_operations_test.go:524 | Active rebuild pins prevent compaction past the rebuild boundary. | cursor-monotonicity | Mutant compacts past an active rebuild pin. |
| REF-072 | conformance/reference/client_operations_test.go:559 | Strict-invalid payloads are rejected for every registered operation. | ENGINE-GAP | Mutant accepts malformed operation input. |
| REF-073 | conformance/reference/client_operations_test.go:562 | Strict-invalid payloads do not mutate the model. | no-state-forks | Mutant partially applies malformed input. |
| REF-074 | conformance/reference/clock_test.go:22 | Injected clocks return the exact controlled time. | oracle-internal | Mutant reads wall-clock time instead of the injected clock. |
| REF-075 | conformance/reference/clock_test.go:36 | Equal seeds and mint sequences produce equal opaque labels. | oracle-internal | Mutant makes token labels nondeterministic. |
| REF-076 | conformance/reference/clock_test.go:39 | Successive token mints produce distinct labels. | oracle-internal | Mutant reuses an opaque token label. |
| REF-077 | conformance/reference/clock_test.go:52 | Token-authority cloning succeeds. | oracle-internal | Mutant rejects cloning of a valid authority. |
| REF-078 | conformance/reference/clock_test.go:55 | Cloned authorities validate copied records. | oracle-internal | Mutant omits records from the clone. |
| REF-079 | conformance/reference/clock_test.go:60 | Clone-only mints do not alter the source authority. | oracle-internal | Mutant aliases the clone mint map. |
| REF-080 | conformance/reference/clock_test.go:64 | Cloning preserves the next token sequence. | oracle-internal | Mutant changes token allocation after cloning. |
| REF-081 | conformance/reference/clock_test.go:67 | Source-only mints do not alter the clone authority. | oracle-internal | Mutant aliases source and clone state. |
| REF-082 | conformance/reference/clock_test.go:119 | Valid tokens return valid status. | oracle-internal | Mutant rejects a correctly bound token. |
| REF-083 | conformance/reference/clock_test.go:123 | Wrong token kinds return wrong-kind status. | oracle-internal | Mutant accepts a token under another kind. |
| REF-084 | conformance/reference/clock_test.go:127 | Fabricated tokens return forged status. | oracle-internal | Mutant accepts an unminted token. |
| REF-085 | conformance/reference/clock_test.go:133 | Request identity changes return misbound status. | oracle-internal | Mutant accepts a token for another user. |
| REF-086 | conformance/reference/clock_test.go:139 | State-lineage changes return stale status. | oracle-internal | Mutant accepts a token from stale schema lineage. |
| REF-087 | conformance/reference/clock_test.go:150 | Binding-presence changes in request context return misbound status. | oracle-internal | Mutant ignores request binding presence. |
| REF-088 | conformance/reference/clock_test.go:159 | Binding-presence changes in state context return stale status. | oracle-internal | Mutant ignores stale state binding presence. |
| REF-089 | conformance/reference/clock_test.go:193 | Absent binding values canonicalize to equivalent bindings. | oracle-internal | Mutant compares hidden absent values. |
| REF-090 | conformance/reference/clock_test.go:213 | Equivalent binding times validate successfully. | oracle-internal | Mutant treats equivalent UTC times as different. |
| REF-091 | conformance/reference/clock_test.go:219 | Issued-time changes return misbound status. | oracle-internal | Mutant classifies request issue-time changes as stale. |
| REF-092 | conformance/reference/clock_test.go:225 | Expiry changes return stale status. | oracle-internal | Mutant classifies expiry changes as request misbinding. |
| REF-093 | conformance/reference/clock_test.go:244 | Current protected times validate before expiry. | oracle-internal | Mutant rejects an unexpired protected-time token. |
| REF-094 | conformance/reference/clock_test.go:248 | Expired protected-time tokens return stale status. | oracle-internal | Mutant accepts an expired token. |
| REF-095 | conformance/reference/clock_test.go:253 | Current-user changes return misbound status. | oracle-internal | Mutant accepts a current token for another user. |
| REF-096 | conformance/reference/clock_test.go:261 | Unsupported mint kinds return a zero token. | oracle-internal | Mutant mints a token for an unsupported kind. |
| REF-097 | conformance/reference/clock_test.go:264 | Zero tokens from unsupported mint kinds validate as forged. | oracle-internal | Mutant treats a zero token as valid. |
| REF-098 | conformance/reference/clock_test.go:272 | Unsupported requested kinds return wrong-kind status. | oracle-internal | Mutant validates a token under an unsupported kind. |
| REF-099 | conformance/reference/clock_test.go:283 | Seed transaction-nonce changes return misbound status. | oracle-internal | Mutant accepts a receipt for another transaction nonce. |
| REF-100 | conformance/reference/clock_test.go:291 | Seed manifest changes return stale status. | oracle-internal | Mutant accepts a receipt for another manifest digest. |
| REF-101 | conformance/reference/clock_test.go:310 | Restored current labels validate. | oracle-internal | Mutant loses current reservations during restore. |
| REF-102 | conformance/reference/clock_test.go:313 | Restored labels from another namespace validate. | oracle-internal | Mutant rejects preserved cross-namespace reservations. |
| REF-103 | conformance/reference/clock_test.go:322 | Restored minting avoids collisions and remains deterministic. | oracle-internal | Mutant collides with a restored label or changes sequence. |
| REF-104 | conformance/reference/clock_test.go:330 | Equivalent restorations produce equal labels. | oracle-internal | Mutant makes restoration order affect labels. |
| REF-105 | conformance/reference/clock_test.go:341 | Identical duplicate reservations are accepted. | oracle-internal | Mutant rejects harmless identical reservations. |
| REF-106 | conformance/reference/clock_test.go:371 | Conflicting, zero, and unsupported reservations are rejected. | oracle-internal | Mutant accepts an invalid restoration reservation. |
| REF-107 | conformance/reference/clock_test.go:389 | Exhausted namespaces return no new token. | oracle-internal | Mutant wraps the token sequence. |
| REF-108 | conformance/reference/clock_test.go:392 | Exhausted namespaces preserve the last restored record. | oracle-internal | Mutant overwrites a restored record at exhaustion. |
| REF-109 | conformance/reference/connect.go:91 | Connect requires a current immutable schema. | ENGINE-GAP | Mutant connects without an installed current schema. |
| REF-110 | conformance/reference/connect.go:117 | Expiry handling requires the current generation history entry. | cursor-monotonicity | Mutant renews a generation absent from history. |
| REF-111 | conformance/reference/connect.go:198 | Current-schema connect requires local schema state. | no-state-forks | Mutant reports success without local schema state. |
| REF-112 | conformance/reference/connect.go:243 | Connect validates runtime version range. | ENGINE-GAP | Mutant accepts a runtime counter above the safe range. |
| REF-113 | conformance/reference/connect.go:246 | Connect requires protocol version. | ENGINE-GAP | Mutant accepts a missing protocol version. |
| REF-114 | conformance/reference/connect.go:250 | Connect requires schema-reset presence. | ENGINE-GAP | Mutant defaults a missing schema-reset flag. |
| REF-115 | conformance/reference/connect.go:253 | Connect requires a schema reference. | ENGINE-GAP | Mutant accepts a missing schema reference. |
| REF-116 | conformance/reference/connect.go:256 | Connect validates scope-set version range. | scope-isolation | Mutant accepts an out-of-range scope-set version. |
| REF-117 | conformance/reference/connect.go:259 | Connect requires known scopes. | scope-isolation | Mutant treats missing known scopes as empty. |
| REF-118 | conformance/reference/connect.go:262 | Connect validates optional client generation. | cursor-monotonicity | Mutant accepts zero or oversized client generations. |
| REF-119 | conformance/reference/connect.go:267 | Connect rejects empty known scope identifiers. | scope-isolation | Mutant accepts an empty known scope. |
| REF-120 | conformance/reference/connect.go:271 | Connect rejects duplicate known scopes. | scope-isolation | Mutant accepts a duplicate known scope. |
| REF-121 | conformance/reference/connect.go:281 | Connect rejects an empty seed-receipt map. | ENGINE-GAP | Mutant treats an empty present receipt map as valid. |
| REF-122 | conformance/reference/connect.go:285 | Connect validates seed receipt scopes and source kinds. | scope-isolation | Mutant accepts an invalid receipt scope or source. |
| REF-123 | conformance/reference/connect.go:323 | Connect rejects an illegal local lifecycle. | ENGINE-GAP | Mutant connects from a stopped or error lifecycle. |
| REF-124 | conformance/reference/connect.go:371 | Connect requires known scopes to match local assignments in count. | scope-isolation | Mutant ignores an assignment-count mismatch. |
| REF-125 | conformance/reference/connect.go:375 | Connect requires known scopes to match local assignments by value. | scope-isolation | Mutant accepts a scope identity mismatch. |
| REF-126 | conformance/reference/connect.go:587 | Cursor bindings require complete client, scope, and schema identity. | cursor-monotonicity | Mutant mints a cursor with incomplete bindings. |
| REF-127 | conformance/reference/connect.go:591 | Cursor bindings require current scope lineage. | cursor-monotonicity | Mutant mints a cursor for obsolete scope generations. |
| REF-128 | conformance/reference/connect.go:594 | Cursor positions require the scope stream generation. | cursor-monotonicity | Mutant accepts a cursor from another stream. |
| REF-129 | conformance/reference/connect.go:647 | Authoritative assignments require complete scope lineage. | scope-isolation | Mutant installs an incomplete assignment. |
| REF-130 | conformance/reference/connect.go:689 | Cursor plans require active server and local assignments. | scope-isolation | Mutant applies a plan to an unassigned scope. |
| REF-131 | conformance/reference/connect.go:696 | Issued cursor plans require replacement. | cursor-monotonicity | Mutant accepts an issued plan without replacement. |
| REF-132 | conformance/reference/connect.go:712 | Unchanged cursor plans cannot hide state changes. | cursor-monotonicity | Mutant changes an unchanged cursor plan. |
| REF-133 | conformance/reference/connect.go:716 | Rebuild-required cursor plans must invalidate the cursor. | cursor-monotonicity | Mutant marks rebuild required without invalidating progress. |
| REF-134 | conformance/reference/connect.go:725 | Unknown cursor dispositions are rejected. | ENGINE-GAP | Mutant silently accepts an unknown disposition. |
| REF-135 | conformance/reference/import_guard_test.go:22 | Reference imports obey the package isolation policy. | oracle-internal | Mutant adds a forbidden dependency to reference. |
| REF-136 | conformance/reference/install.go:178 | Contract installation requires an empty protocol state. | no-state-forks | Mutant installs over existing protocol state. |
| REF-137 | conformance/reference/install.go:189 | Initial schema publication requires class initial. | ENGINE-GAP | Mutant installs a noninitial first schema. |
| REF-138 | conformance/reference/install.go:193 | Initial schema reference must be normal and valid. | ENGINE-GAP | Mutant accepts a fresh or malformed initial reference. |
| REF-139 | conformance/reference/install.go:200 | Initial schema cannot declare affected scopes. | scope-isolation | Mutant assigns affected scopes during initial install. |
| REF-140 | conformance/reference/install.go:248 | Installation limits require every configured field. | ENGINE-GAP | Mutant accepts incomplete configured limits. |
| REF-141 | conformance/reference/install.go:257 | Installation limits equal the Protocol 3 maxima. | ENGINE-GAP | Mutant installs noncontract limits. |
| REF-142 | conformance/reference/install.go:264 | Installation capability input requires every field. | ENGINE-GAP | Mutant accepts incomplete installation metadata. |
| REF-143 | conformance/reference/install.go:268 | Installation capability values match Protocol 3 requirements. | ENGINE-GAP | Mutant installs incompatible capability values. |
| REF-144 | conformance/reference/install.go:276 | Installation rejects empty endpoints. | ENGINE-GAP | Mutant accepts an empty endpoint. |
| REF-145 | conformance/reference/install.go:279 | Installation rejects duplicate endpoints. | ENGINE-GAP | Mutant accepts duplicate endpoint definitions. |
| REF-146 | conformance/reference/install.go:289 | Installation capability entries require identity and enabled state. | ENGINE-GAP | Mutant accepts an incomplete capability entry. |
| REF-147 | conformance/reference/install.go:293 | Installation rejects duplicate capability identities. | ENGINE-GAP | Mutant accepts duplicate capability definitions. |
| REF-148 | conformance/reference/install.go:305 | Stream installation requires generation, database, worker, and slot. | ENGINE-GAP | Mutant installs an incomplete stream authority. |
| REF-149 | conformance/reference/install.go:320 | Initial registry requires a positive generation and all rule sets. | ENGINE-GAP | Mutant installs an incomplete registry. |
| REF-150 | conformance/reference/install.go:331 | Initial registry rejects duplicate relation identities. | scope-isolation | Mutant aliases two registry relations. |
| REF-151 | conformance/reference/install.go:335 | Initial registry rejects duplicate physical identities. | scope-isolation | Mutant registers one physical relation twice. |
| REF-152 | conformance/reference/install.go:339 | Initial registry rejects duplicate table identities. | scope-isolation | Mutant maps two relations to one table. |
| REF-153 | conformance/reference/install.go:393 | Relation installation requires a complete closed shape and bounded fanout. | ENGINE-GAP | Mutant installs an incomplete or overbound relation. |
| REF-154 | conformance/reference/install.go:419 | Synced relations require valid table and primary-key bindings. | scope-isolation | Mutant installs a synced relation with capture-only bindings. |
| REF-155 | conformance/reference/install.go:432 | Capture dependencies require null table bindings and capture keys. | scope-isolation | Mutant installs capture dependency as a synced table. |
| REF-156 | conformance/reference/install.go:438 | Dependency function and bound presence must agree. | ENGINE-GAP | Mutant accepts only one half of a dependency binding. |
| REF-157 | conformance/reference/install.go:442 | Dependency bindings require nonempty fields and bounded rows. | ENGINE-GAP | Mutant accepts an invalid dependency impact bound. |
| REF-158 | conformance/reference/install.go:447 | Null dependency impact requires no dependency fields. | ENGINE-GAP | Mutant accepts fields without a dependency impact function. |
| REF-159 | conformance/reference/install.go:454 | Physical relations require complete identity and a known replica mode. | ENGINE-GAP | Mutant installs an incomplete physical relation. |
| REF-160 | conformance/reference/install.go:470 | Synced relation metadata matches its schema table. | scope-isolation | Mutant binds a relation to another schema table. |
| REF-161 | conformance/reference/install.go:474 | Synced relation primary-key metadata matches its schema field. | scope-isolation | Mutant binds a relation to an incompatible primary key. |
| REF-162 | conformance/reference/install.go:484 | Required field lists cannot be empty. | ENGINE-GAP | Mutant accepts an empty captured-field list. |
| REF-163 | conformance/reference/install.go:491 | Field lists reject empty and duplicate identifiers. | ENGINE-GAP | Mutant accepts an empty or duplicate field identifier. |
| REF-164 | conformance/reference/install.go:508 | Capture dependencies require complete identities. | scope-isolation | Mutant accepts an incomplete dependency edge. |
| REF-165 | conformance/reference/install.go:514 | Capture dependencies reject duplicates and self-reference. | scope-isolation | Mutant installs a duplicate or self-referential edge. |
| REF-166 | conformance/reference/install.go:519 | Capture dependencies connect capture relations to synced relations. | scope-isolation | Mutant connects invalid relation kinds. |
| REF-167 | conformance/reference/install.go:522 | Capture dependencies reject duplicate edges. | scope-isolation | Mutant installs the same dependency edge twice. |
| REF-168 | conformance/reference/install.go:535 | Scope rules are unique and within fanout limits. | scope-isolation | Mutant accepts duplicate or overbound scope rules. |
| REF-169 | conformance/reference/install.go:542 | Dependency impacts are unique and within row limits. | scope-isolation | Mutant accepts duplicate or overbound impacts. |
| REF-170 | conformance/reference/install.go:556 | Empty scopes require complete positive lineage. | scope-isolation | Mutant installs an incomplete scope. |
| REF-171 | conformance/reference/install.go:560 | Empty scopes reject duplicate identities. | scope-isolation | Mutant installs one scope twice. |
| REF-172 | conformance/reference/install.go:584 | Clients require complete unique identity and local-ready state. | ENGINE-GAP | Mutant installs an incomplete or duplicate client. |
| REF-173 | conformance/reference/install.go:588 | Client local schema must equal the initial schema. | no-state-forks | Mutant installs divergent server and local schemas. |
| REF-174 | conformance/reference/install.go:595 | Client acknowledgement timestamps must use RFC3339Nano. | ENGINE-GAP | Mutant accepts an invalid acknowledgement timestamp. |
| REF-175 | conformance/reference/install.go:607 | Client assignments must reference known scopes. | scope-isolation | Mutant assigns an unknown scope. |
| REF-176 | conformance/reference/install.go:610 | Client assignments reject duplicate scopes. | scope-isolation | Mutant installs a duplicate assignment. |
| REF-177 | conformance/reference/install.go:635 | Write policies require complete identity and decision. | ENGINE-GAP | Mutant accepts an incomplete write policy. |
| REF-178 | conformance/reference/install.go:639 | Write policies require known tables. | scope-isolation | Mutant authorizes a table absent from the schema. |
| REF-179 | conformance/reference/install.go:643 | Write policies reject duplicate user-table entries. | scope-isolation | Mutant accepts conflicting duplicate policy entries. |
| REF-180 | conformance/reference/membership.go:74 | Staged registry generations require a positive portable registry generation. | cursor-monotonicity | Mutant accepts zero or oversized registry generations. |
| REF-181 | conformance/reference/membership.go:77 | Staged membership generations require a positive portable generation. | cursor-monotonicity | Mutant accepts zero or oversized membership generations. |
| REF-182 | conformance/reference/membership.go:84 | Activation boundaries equal the complete durable materialization boundary. | cursor-monotonicity | Mutant stages a generation at an unmaterialized boundary. |
| REF-183 | conformance/reference/membership.go:98 | Registry generations are immutable. | no-state-forks | Mutant overwrites an existing registry generation. |
| REF-184 | conformance/reference/membership.go:101 | Registry generations increase monotonically. | cursor-monotonicity | Mutant stages a generation older than the active generation. |
| REF-185 | conformance/reference/membership.go:106 | Staging requires an active validated current generation. | ENGINE-GAP | Mutant stages from an unvalidated or bootstrap generation. |
| REF-186 | conformance/reference/membership.go:141 | Affected scopes receive increasing membership generations. | cursor-monotonicity | Mutant reuses a scope membership generation. |
| REF-187 | conformance/reference/membership.go:149 | Candidate membership changes remain isolated to affected scopes. | scope-isolation | Mutant changes an unlisted scope during staging. |
| REF-188 | conformance/reference/membership.go:195 | Activation requires a verified candidate membership stage. | ENGINE-GAP | Mutant activates an unverified candidate. |
| REF-189 | conformance/reference/membership.go:198 | Candidate generation and activation bindings remain consistent. | no-state-forks | Mutant activates a candidate with mismatched lineage. |
| REF-190 | conformance/reference/membership.go:201 | Main WAL materialization must reach the activation boundary. | cursor-monotonicity | Mutant activates before WAL reaches the barrier. |
| REF-191 | conformance/reference/membership.go:210 | Candidate scopes require complete active-stream lineage. | scope-isolation | Mutant activates an incomplete candidate scope. |
| REF-192 | conformance/reference/membership.go:213 | Candidate scopes are unique. | scope-isolation | Mutant activates duplicate candidate scopes. |
| REF-193 | conformance/reference/membership.go:241 | Activation boundaries require all stream-position fields. | cursor-monotonicity | Mutant accepts an incomplete activation boundary. |
| REF-194 | conformance/reference/membership.go:244 | Activation boundaries use transaction-end positions in the active stream. | cursor-monotonicity | Mutant accepts an effect or foreign-stream boundary. |
| REF-195 | conformance/reference/membership.go:255 | Affected scope declarations are nonempty. | scope-isolation | Mutant stages with no affected scopes. |
| REF-196 | conformance/reference/membership.go:264 | Affected scopes must be authoritative. | scope-isolation | Mutant stages an unknown scope. |
| REF-197 | conformance/reference/membership.go:267 | Affected scopes reject duplicates. | scope-isolation | Mutant stages one scope twice. |
| REF-198 | conformance/reference/membership.go:296 | Scope-rule payloads require complete bounded identity. | scope-isolation | Mutant accepts an incomplete scope rule. |
| REF-199 | conformance/reference/membership.go:306 | Scope rules target synced registrations. | scope-isolation | Mutant applies a scope rule to a capture dependency. |
| REF-200 | conformance/reference/membership.go:316 | Scope-rule evaluations use valid rows from the registered table. | scope-isolation | Mutant accepts a row from another table. |
| REF-201 | conformance/reference/membership.go:331 | Scope-rule fanout stays within its positive bound. | scope-isolation | Mutant accepts an overfanout evaluation. |
| REF-202 | conformance/reference/membership.go:351 | Dependency-impact payloads require complete bounded identity. | scope-isolation | Mutant accepts an incomplete dependency impact. |
| REF-203 | conformance/reference/membership.go:382 | Dependency-impact rows are valid registered rows. | scope-isolation | Mutant accepts an undeclared affected row. |
| REF-204 | conformance/reference/membership.go:387 | Dependency-impact row counts stay within their positive bound. | scope-isolation | Mutant accepts an overbound impact. |
| REF-205 | conformance/reference/membership.go:464 | Unaffected scopes retain equal membership sets. | scope-isolation | Mutant changes membership in an unaffected scope. |
| REF-206 | conformance/reference/model.go:47 | Model creation rejects unsupported protocol versions. | oracle-internal | Mutant creates a model for another protocol version. |
| REF-207 | conformance/reference/model.go:50 | Model creation requires a non-nil clock, including typed nils. | oracle-internal | Mutant creates a model without a usable clock. |
| REF-208 | conformance/reference/model.go:53 | Model creation validates initial state before cloning it. | oracle-internal | Mutant accepts invalid initial state. |
| REF-209 | conformance/reference/model.go:87 | Applying an unknown operation returns the closed-registry error. | oracle-internal | Mutant dispatches an unregistered operation. |
| REF-210 | conformance/reference/model.go:90 | Resolved input is allowed only for the two resolved operations. | oracle-internal | Mutant accepts resolved input on another operation. |
| REF-211 | conformance/reference/model.go:187 | Handlers must return a valid closed result shape. | oracle-internal | Mutant commits an invalid result shape. |
| REF-212 | conformance/reference/model.go:190 | Pull hydration faults must select their requested projection. | oracle-internal | Mutant silently ignores a requested hydration fault. |
| REF-213 | conformance/reference/model.go:203 | State and token authority commit only after all checks pass. | oracle-internal | Mutant commits a partially successful operation. |
| REF-214 | conformance/reference/model.go:243 | Authoritative rows require nonempty matching canonical identities. | oracle-internal | Mutant accepts a row whose map key differs from its identity. |
| REF-215 | conformance/reference/model.go:249 | Authoritative rows have unique canonical identity bytes. | oracle-internal | Mutant accepts two rows with one canonical identity. |
| REF-216 | conformance/reference/model.go:259 | Relation map keys match relation definitions. | oracle-internal | Mutant accepts a relation under another map key. |
| REF-217 | conformance/reference/model.go:279 | Synced relations require table identities. | oracle-internal | Mutant accepts a synced relation without a table. |
| REF-218 | conformance/reference/model.go:283 | Capture dependencies cannot carry table identities. | oracle-internal | Mutant accepts a capture dependency with a table. |
| REF-219 | conformance/reference/model.go:287 | Unknown registration kinds are rejected. | oracle-internal | Mutant accepts an unknown relation registration kind. |
| REF-220 | conformance/reference/model.go:295 | Initial state contains no configured server token. | oracle-internal | Mutant accepts hidden or present tokens in initial state. |
| REF-221 | conformance/reference/model_test.go:42 | New does not read the injected clock. | oracle-internal | Mutant reads time while constructing the model. |
| REF-222 | conformance/reference/model_test.go:45 | New preserves the initial state snapshot. | oracle-internal | Mutant changes state during model construction. |
| REF-223 | conformance/reference/model_test.go:54 | New reports invalid initial state for unsupported versions. | oracle-internal | Mutant returns another error or succeeds. |
| REF-224 | conformance/reference/model_test.go:69 | New rejects nil and typed-nil clocks. | oracle-internal | Mutant accepts an unusable clock. |
| REF-225 | conformance/reference/model_test.go:132 | Every configured token family is rejected in initial state. | oracle-internal | Mutant accepts a configured token family. |
| REF-226 | conformance/reference/model_test.go:194 | Hidden token values without presence are rejected. | oracle-internal | Mutant accepts hidden token state. |
| REF-227 | conformance/reference/model_test.go:228 | Invalid authoritative rows are rejected. | oracle-internal | Mutant accepts malformed or duplicate authoritative identity. |
| REF-228 | conformance/reference/model_test.go:261 | Invalid relation registrations are rejected. | oracle-internal | Mutant accepts an inconsistent relation registration. |
| REF-229 | conformance/reference/model_test.go:276 | Config state mutation does not alter model state. | oracle-internal | Mutant aliases model state to configuration state. |
| REF-230 | conformance/reference/model_test.go:292 | Snapshot mutation does not alter model state or later snapshots. | oracle-internal | Mutant returns aliased snapshot state. |
| REF-231 | conformance/reference/model_test.go:307 | Equivalent model states produce equal snapshots. | oracle-internal | Mutant exposes map insertion order. |
| REF-232 | conformance/reference/model_test.go:318 | Unknown operation returns the closed-registry error. | oracle-internal | Mutant executes an unknown operation. |
| REF-233 | conformance/reference/model_test.go:321 | Unknown operation leaves model state unchanged. | oracle-internal | Mutant mutates before rejecting dispatch. |
| REF-234 | conformance/reference/model_test.go:343 | Canceled and expired contexts preserve their errors. | oracle-internal | Mutant replaces context errors. |
| REF-235 | conformance/reference/model_test.go:346 | Context errors leave model state unchanged. | oracle-internal | Mutant commits after context cancellation. |
| REF-236 | conformance/reference/model_test.go:373 | A gated apply returns after context cancellation. | oracle-internal | Mutant waits for gate release after cancellation. |
| REF-237 | conformance/reference/model_test.go:386 | Nil context is rejected. | oracle-internal | Mutant dereferences or accepts a nil context. |
| REF-238 | conformance/reference/model_test.go:389 | Nil-context rejection leaves state unchanged. | oracle-internal | Mutant mutates before nil-context validation. |
| REF-239 | conformance/reference/model_test.go:401 | Equal model seeds produce equal token labels. | oracle-internal | Mutant makes model token labels seed-independent. |
| REF-240 | conformance/reference/operations.go:242 | Operation registration requires a nonempty key and handler. | oracle-internal | Mutant registers an invalid operation entry. |
| REF-241 | conformance/reference/operations.go:245 | Operation registration rejects duplicate keys. | oracle-internal | Mutant silently replaces an operation handler. |
| REF-242 | conformance/reference/operations.go:267 | Strict JSON validation rejects invalid JSON values. | oracle-internal | Mutant accepts malformed JSON. |
| REF-243 | conformance/reference/operations.go:274 | Strict decoding rejects unknown fields and type errors. | oracle-internal | Mutant accepts unknown fields or invalid types. |
| REF-244 | conformance/reference/operations.go:278 | Strict decoding rejects trailing JSON values. | oracle-internal | Mutant accepts multiple JSON documents. |
| REF-245 | conformance/reference/operations.go:308 | Contract-installed results contain no HTTP or domain observation. | oracle-internal | Mutant attaches an observation to contract installation. |
| REF-246 | conformance/reference/operations.go:312 | Connect results contain exactly HTTP and connect observations. | oracle-internal | Mutant accepts a malformed connect result. |
| REF-247 | conformance/reference/operations.go:316 | Local results contain exactly one local observation and no HTTP. | oracle-internal | Mutant accepts a malformed local result. |
| REF-248 | conformance/reference/operations.go:320 | Lifecycle results contain exactly one lifecycle observation and no HTTP. | oracle-internal | Mutant accepts a malformed lifecycle result. |
| REF-249 | conformance/reference/operations.go:324 | Push results contain exactly HTTP and push observations. | oracle-internal | Mutant accepts a malformed push result. |
| REF-250 | conformance/reference/operations.go:328 | Pull results contain exactly HTTP and pull observations. | oracle-internal | Mutant accepts a malformed pull result. |
| REF-251 | conformance/reference/operations.go:332 | Rebuild results contain exactly HTTP and rebuild observations. | oracle-internal | Mutant accepts a malformed rebuild result. |
| REF-252 | conformance/reference/operations.go:336 | WAL results contain exactly one WAL observation and no HTTP. | oracle-internal | Mutant accepts a malformed WAL result. |
| REF-253 | conformance/reference/operations.go:340 | Schema results contain exactly one schema observation and no HTTP. | oracle-internal | Mutant accepts a malformed schema result. |
| REF-254 | conformance/reference/operations.go:344 | Retention results contain exactly one retention observation and no HTTP. | oracle-internal | Mutant accepts a malformed retention result. |
| REF-255 | conformance/reference/operations.go:348 | Client results contain exactly one client observation and no HTTP. | oracle-internal | Mutant accepts a malformed client result. |
| REF-256 | conformance/reference/operations.go:352 | Unknown result kinds are rejected. | oracle-internal | Mutant accepts an unknown result kind. |
| REF-257 | conformance/reference/operations.go:361 | HTTP code presence matches the code value. | oracle-internal | Mutant permits inconsistent HTTP code presence. |
| REF-258 | conformance/reference/operations.go:364 | Hidden retry-after values are rejected. | oracle-internal | Mutant accepts an unmarked retry-after value. |
| REF-259 | conformance/reference/operations_test.go:18 | Every scenario operation has a reference class. | oracle-internal | Mutant leaves a scenario operation unclassified. |
| REF-260 | conformance/reference/operations_test.go:27 | The operation registry has exactly the expected key set. | oracle-internal | Mutant adds or removes a registry operation. |
| REF-261 | conformance/reference/operations_test.go:31 | Every expected operation has a handler. | oracle-internal | Mutant registers a nil handler. |
| REF-262 | conformance/reference/operations_test.go:46 | Registry map mutation does not alter later registry copies. | oracle-internal | Mutant returns the live registry map. |
| REF-263 | conformance/reference/operations_test.go:59 | Configured installation is rejected. | oracle-internal | Mutant reinstalls an already configured contract. |
| REF-264 | conformance/reference/operations_test.go:62 | Rejected installation leaves configured state unchanged. | oracle-internal | Mutant partially overwrites configured state. |
| REF-265 | conformance/reference/operations_test.go:81 | Invalid install payloads are rejected. | oracle-internal | Mutant accepts malformed installation input. |
| REF-266 | conformance/reference/operations_test.go:84 | Invalid install payloads leave state unchanged. | oracle-internal | Mutant commits partial install state. |
| REF-267 | conformance/reference/operations_test.go:105 | Strict decoding preserves large typed numbers and nested values. | oracle-internal | Mutant rounds or drops typed JSON values. |
| REF-268 | conformance/reference/operations_test.go:119 | Strict decoding rejects duplicate, unknown, trailing, null, and invalid-byte payloads. | oracle-internal | Mutant accepts one strict-invalid payload class. |
| REF-269 | conformance/reference/operations_test.go:138 | Handler errors propagate unchanged. | oracle-internal | Mutant replaces handler errors. |
| REF-270 | conformance/reference/operations_test.go:140 | Failed handlers roll back state and token minting. | oracle-internal | Mutant commits state or tokens after handler failure. |
| REF-271 | conformance/reference/operations_test.go:158 | Cancellation errors propagate from handlers. | oracle-internal | Mutant hides handler cancellation. |
| REF-272 | conformance/reference/operations_test.go:160 | Cancellation rolls back state and token minting. | oracle-internal | Mutant commits canceled handler work. |
| REF-273 | conformance/reference/operations_test.go:176 | Successful handlers return their declared result kind. | oracle-internal | Mutant changes the successful result kind. |
| REF-274 | conformance/reference/operations_test.go:183 | Successful handlers commit state exactly once. | oracle-internal | Mutant commits zero or multiple times. |
| REF-275 | conformance/reference/operations_test.go:190 | Successful handlers commit the first token mint. | oracle-internal | Mutant loses the committed token mint. |
| REF-276 | conformance/reference/operations_test.go:193 | Successful handlers advance token sequence once. | oracle-internal | Mutant advances token sequence more than once. |
| REF-277 | conformance/reference/operations_test.go:235 | Invalid result shapes are rejected and rolled back. | oracle-internal | Mutant commits an invalid result shape. |
| REF-278 | conformance/reference/pull.go:96 | Pull rejects a limit above the configured maximum. | ENGINE-GAP | Mutant accepts an oversized pull. |
| REF-279 | conformance/reference/pull.go:112 | Pull rejects duplicate requested scopes. | scope-isolation | Mutant processes one requested scope twice. |
| REF-280 | conformance/reference/pull.go:116 | Pull requires every requested scope assignment. | scope-isolation | Mutant pulls an unassigned scope. |
| REF-281 | conformance/reference/pull.go:136 | Pull requires authoritative state for assigned scopes. | scope-isolation | Mutant fabricates missing scope state. |
| REF-282 | conformance/reference/pull.go:149 | Pull rejects missing cursor tokens. | cursor-monotonicity | Mutant treats a missing cursor as valid. |
| REF-283 | conformance/reference/pull.go:159 | Pull rejects forged or misbound cursors. | cursor-monotonicity | Mutant accepts a forged cursor. |
| REF-284 | conformance/reference/pull.go:178 | Pull requires authoritative state for implicitly added scopes. | scope-isolation | Mutant adds a scope without state. |
| REF-285 | conformance/reference/pull.go:200 | Pull blocks while an accepted fence lacks coverage. | mutation-conservation | Mutant exposes a pull before accepted writes materialize. |
| REF-286 | conformance/reference/pull.go:211 | Pull rejects invalid candidate integrity. | mutation-conservation | Mutant emits an invalid scope effect. |
| REF-287 | conformance/reference/pull.go:237 | Pull rejects missing projection hydration. | checksum-convergence | Mutant hydrates a change without its captured projection. |
| REF-288 | conformance/reference/pull.go:249 | Terminal pull requires every active scope state. | checksum-convergence | Mutant emits a terminal result with missing scope state. |
| REF-289 | conformance/reference/pull.go:266 | Pull does not issue a cursor without progress. | cursor-monotonicity | Mutant issues a cursor at the current position. |
| REF-290 | conformance/reference/pull.go:274 | Pull rejects token-authority exhaustion before mutation. | no-state-forks | Mutant partially commits a pull when token minting is exhausted. |
| REF-291 | conformance/reference/pull.go:286 | Pull rejects a zero cursor minted by the authority. | cursor-monotonicity | Mutant stores a zero pull cursor. |
| REF-292 | conformance/reference/pull.go:299 | Completed rebuild requirements clear only after valid acknowledgement. | cursor-monotonicity | Mutant clears rebuild state without valid final progress. |
| REF-293 | conformance/reference/pull.go:348 | Local pull apply requires complete identity. | oracle-internal | Mutant applies a page with incomplete identity. |
| REF-294 | conformance/reference/pull.go:352 | Local pull apply accepts only a resolved source step. | oracle-internal | Mutant applies a page without a source step. |
| REF-295 | conformance/reference/pull.go:356 | Local pull apply binds to the exact pull source step. | no-state-forks | Mutant applies a page from another step. |
| REF-296 | conformance/reference/pull.go:359 | Local pull apply requires a successful pull source result. | no-state-forks | Mutant applies an error or non-pull result. |
| REF-297 | conformance/reference/pull.go:434 | Local pull apply rejects duplicate changes. | mutation-conservation | Mutant applies one scoped row change twice. |
| REF-298 | conformance/reference/pull.go:438 | Local pull apply requires complete valid changes. | mutation-conservation | Mutant applies an incomplete or invalid change. |
| REF-299 | conformance/reference/pull.go:447 | Local pull apply requires matching current projection hydration. | checksum-convergence | Mutant applies a change from a different projection. |
| REF-300 | conformance/reference/pull.go:507 | Local pull apply rejects invalid cursor dispositions. | cursor-monotonicity | Mutant accepts an unknown cursor disposition. |
| REF-301 | conformance/reference/pull.go:519 | Local pull apply requires the issued server cursor to be current. | cursor-monotonicity | Mutant installs a stale server cursor. |
| REF-302 | conformance/reference/pull.go:522 | Local pull apply rejects backward local progress. | cursor-monotonicity | Mutant moves a local checkpoint backward. |
| REF-303 | conformance/reference/pull.go:532 | Nonterminal pull pages contain no checksums. | checksum-convergence | Mutant verifies checksums before terminal convergence. |
| REF-304 | conformance/reference/pull.go:538 | Terminal pull pages contain one checksum per active scope. | checksum-convergence | Mutant accepts an incomplete terminal checksum set. |
| REF-305 | conformance/reference/pull.go:552 | Terminal checksums equal authoritative scope checksums. | checksum-convergence | Mutant accepts a changed terminal checksum. |
| REF-306 | conformance/reference/pull.go:571 | Local pull apply rejects changed scope lineage. | scope-isolation | Mutant applies data after assignment lineage changes. |
| REF-307 | conformance/reference/pull.go:632 | Pull validates identity and safe client generation range. | ENGINE-GAP | Mutant accepts invalid pull identity. |
| REF-308 | conformance/reference/pull.go:635 | Pull requires a positive limit and bounded scope-set version. | ENGINE-GAP | Mutant accepts zero or oversized pull limits. |
| REF-309 | conformance/reference/pull.go:638 | Pull requires scopes. | scope-isolation | Mutant accepts a missing scope list. |
| REF-310 | conformance/reference/pull.go:645 | Pull validates every scope token source. | cursor-monotonicity | Mutant accepts an unknown cursor source. |
| REF-311 | conformance/reference/pull_fault_test.go:40 | Hydration setup produces exactly one captured projection. | checksum-convergence | Mutant changes projection cardinality during setup. |
| REF-312 | conformance/reference/pull_fault_test.go:48 | A selected hydration fault returns integrity failure. | checksum-convergence | Mutant accepts a missing selected projection. |
| REF-313 | conformance/reference/pull_fault_test.go:51 | A rejected hydration fault leaves durable state unchanged. | no-state-forks | Mutant commits state after hydration failure. |
| REF-314 | conformance/reference/pull_fault_test.go:59 | Retrying pull without the fault succeeds normally. | checksum-convergence | Mutant leaves a transient hydration fault durable. |
| REF-315 | conformance/reference/pull_fault_test.go:75 | An unselected hydration fault is rejected without state change. | no-state-forks | Mutant applies a fault to the wrong projection. |
| REF-316 | conformance/reference/pull_rebuild_operations_test.go:48 | Oversized pull limits return typed invalid requests without mutation. | no-state-forks | Mutant accepts or partially applies an oversized pull. |
| REF-317 | conformance/reference/pull_rebuild_operations_test.go:80 | Oversized rebuild limits return typed invalid requests without mutation. | no-state-forks | Mutant accepts or partially applies an oversized rebuild. |
| REF-318 | conformance/reference/pull_rebuild_operations_test.go:120 | Pull pagination returns a nonterminal first page. | cursor-monotonicity | Mutant marks the first page terminal. |
| REF-319 | conformance/reference/pull_rebuild_operations_test.go:133 | Pull selects the retained greatest scoped candidate. | mutation-conservation | Mutant selects a raw pre-limit or older effect. |
| REF-320 | conformance/reference/pull_rebuild_operations_test.go:136 | Pull hydrates historical effects from captured projections, not live rows. | checksum-convergence | Mutant hydrates from the current live row. |
| REF-321 | conformance/reference/pull_rebuild_operations_test.go:140 | Nonterminal pulls omit terminal checksums. | checksum-convergence | Mutant emits checksums on a nonterminal page. |
| REF-322 | conformance/reference/pull_rebuild_operations_test.go:164 | Terminal pull returns all active-scope checksums. | checksum-convergence | Mutant omits an active-scope checksum. |
| REF-323 | conformance/reference/pull_rebuild_operations_test.go:184 | Missing projection returns integrity failure without cursor or checkpoint commit. | no-state-forks | Mutant advances progress after hydration failure. |
| REF-324 | conformance/reference/pull_rebuild_operations_test.go:203 | Issued cursors do not advance durable server checkpoints. | cursor-monotonicity | Mutant acknowledges an issued cursor immediately. |
| REF-325 | conformance/reference/pull_rebuild_operations_test.go:222 | Older valid cursors cannot move a checkpoint backward. | cursor-monotonicity | Mutant rewinds a server checkpoint. |
| REF-326 | conformance/reference/pull_rebuild_operations_test.go:257 | Forged cursors fail without processing another scope or changing state. | cursor-monotonicity | Mutant accepts a forged cursor or leaks another scope. |
| REF-327 | conformance/reference/pull_rebuild_operations_test.go:281 | Capture-pending blocks page, session, and checkpoint allocation. | mutation-conservation | Mutant allocates pull state while capture is pending. |
| REF-328 | conformance/reference/pull_rebuild_operations_test.go:361 | Replayed rebuild pages equal stored pages and preserve the session. | no-state-forks | Mutant regenerates or mutates replayed page content. |
| REF-329 | conformance/reference/pull_rebuild_operations_test.go:441 | Valid final-cursor pull acknowledges the rebuild assignment. | cursor-monotonicity | Mutant leaves rebuild required after final acknowledgement. |
| REF-330 | conformance/reference/pull_rebuild_operations_test.go:601 | Final page apply stores pending finality without installing local progress. | no-state-forks | Mutant installs local cursor before checksum finality. |
| REF-331 | conformance/reference/push.go:258 | Local writes cannot resurrect deleted rows. | mutation-conservation | Mutant changes a tombstone back to live locally. |
| REF-332 | conformance/reference/push.go:264 | Presented base versions must equal local server state. | mutation-conservation | Mutant accepts a stale presented base. |
| REF-333 | conformance/reference/push.go:278 | Inserts reject existing local targets. | mutation-conservation | Mutant overwrites an existing local row on insert. |
| REF-334 | conformance/reference/push.go:287 | Updates require an existing local target. | mutation-conservation | Mutant creates a row for an absent update target. |
| REF-335 | conformance/reference/push.go:294 | Deletes require an existing local target. | mutation-conservation | Mutant queues a delete for an absent target. |
| REF-336 | conformance/reference/push.go:317 | Mutation identifiers cannot be reused locally. | mutation-conservation | Mutant queues duplicate mutation identities. |
| REF-337 | conformance/reference/push.go:404 | Response-loss processing accepts only retryable sealed states. | no-state-forks | Mutant marks a reconciled batch as retryable. |
| REF-338 | conformance/reference/push.go:439 | Batch fingerprints prevent idempotency conflicts. | mutation-conservation | Mutant reuses a batch identity with changed content. |
| REF-339 | conformance/reference/push.go:457 | Mutation fingerprints prevent mutation identity conflicts. | mutation-conservation | Mutant reuses a mutation identity with changed content. |
| REF-340 | conformance/reference/push.go:465 | Expired client generations reject pushes. | cursor-monotonicity | Mutant executes a push under an expired generation. |
| REF-341 | conformance/reference/push.go:470 | Push generation must equal the current client generation. | cursor-monotonicity | Mutant executes a push under another generation. |
| REF-342 | conformance/reference/push.go:474 | Push schema must equal the current schema. | ENGINE-GAP | Mutant executes a push under a stale schema. |
| REF-343 | conformance/reference/push.go:479 | Invalid transaction LSN order returns retryable failure. | cursor-monotonicity | Mutant accepts end LSN before commit LSN. |
| REF-344 | conformance/reference/push.go:555 | Accepted-write epoch cannot overflow. | cursor-monotonicity | Mutant wraps the accepted-write epoch. |
| REF-345 | conformance/reference/push.go:601 | Canonical push responses stay within the response limit. | ENGINE-GAP | Mutant emits an oversized canonical response. |
| REF-346 | conformance/reference/push.go:688 | Push requires active synced WAL capture. | mutation-conservation | Mutant accepts a write without active capture authority. |
| REF-347 | conformance/reference/push.go:705 | Inserts reject existing authoritative rows. | mutation-conservation | Mutant overwrites an authoritative row on insert. |
| REF-348 | conformance/reference/push.go:726 | Updates reject absent rows. | mutation-conservation | Mutant applies an update to an absent row. |
| REF-349 | conformance/reference/push.go:730 | Updates require the current base version. | mutation-conservation | Mutant applies an update from a stale base. |
| REF-350 | conformance/reference/push.go:752 | Deletes reject absent rows. | mutation-conservation | Mutant deletes an absent row. |
| REF-351 | conformance/reference/push.go:756 | Deletes require the current base version. | mutation-conservation | Mutant applies a delete from a stale base. |
| REF-352 | conformance/reference/push.go:1102 | Fence identities cannot collide. | mutation-conservation | Mutant overwrites a write fence. |
| REF-353 | conformance/reference/push.go:1183 | Completed replay requires a successful stored response. | mutation-conservation | Mutant replays an incomplete batch as success. |
| REF-354 | conformance/reference/push_operations_test.go:125 | Local writes capture the expected mutation identity and content. | mutation-conservation | Mutant changes queued mutation content. |
| REF-355 | conformance/reference/push_operations_test.go:135 | Local writes capture exactly the expected authored columns. | mutation-conservation | Mutant drops or adds authored columns. |
| REF-356 | conformance/reference/push_operations_test.go:153 | Server-applied writes create no local mutation state. | mutation-conservation | Mutant queues a mutation during server apply. |
| REF-357 | conformance/reference/push_operations_test.go:157 | Server-applied fields equal authoritative fields. | mutation-conservation | Mutant applies nonauthoritative field values. |
| REF-358 | conformance/reference/push_operations_test.go:245 | Insert-delete normalization avoids a transient sendable mutation. | mutation-conservation | Mutant sends a canceled insert-delete pair. |
| REF-359 | conformance/reference/push_operations_test.go:248 | Insert-delete normalization creates no server state before push. | mutation-conservation | Mutant materializes local normalization on the server. |
| REF-360 | conformance/reference/push_operations_test.go:270 | Normalized inserts do not fabricate a base version. | mutation-conservation | Mutant assigns a base version to an insert. |
| REF-361 | conformance/reference/push_operations_test.go:321 | Accepted predecessors refresh dependent bases. | mutation-conservation | Mutant submits a successor with a stale base. |
| REF-362 | conformance/reference/push_operations_test.go:324 | Base refresh preserves authored successor content. | mutation-conservation | Mutant rewrites authored successor fields. |
| REF-363 | conformance/reference/push_operations_test.go:341 | Predecessor replay preserves its canonical response. | mutation-conservation | Mutant regenerates a historical predecessor response. |
| REF-364 | conformance/reference/push_operations_test.go:344 | Predecessor replay preserves successor state and intent. | mutation-conservation | Mutant reevaluates or changes successor intent. |
| REF-365 | conformance/reference/push_operations_test.go:404 | Accepted and rejected outcomes partition the submitted mutations. | mutation-conservation | Mutant drops or duplicates a push outcome. |
| REF-366 | conformance/reference/push_operations_test.go:568 | Accepted row checksum covers the complete row and opaque version. | checksum-convergence | Mutant computes a partial row checksum. |
| REF-367 | conformance/reference/push_operations_test.go:580 | Accepted pushes create no pull-visible effects before WAL materialization. | mutation-conservation | Mutant exposes an accepted write before WAL. |
| REF-368 | conformance/reference/push_operations_test.go:604 | Accepted mutation fences are present and pending. | mutation-conservation | Mutant omits or prematurely covers a fence. |
| REF-369 | conformance/reference/push_operations_test.go:693 | Completed replay returns the original HTTP body and outcomes. | mutation-conservation | Mutant recomputes a completed response. |
| REF-370 | conformance/reference/push_operations_test.go:703 | Completed replay creates no new server state. | no-state-forks | Mutant executes DML during replay. |
| REF-371 | conformance/reference/push_operations_test.go:720 | Completed replay does not reevaluate changed authority. | mutation-conservation | Mutant changes historical outcomes after authority changes. |
| REF-372 | conformance/reference/rebuild.go:90 | Rebuild rejects a limit above the configured maximum. | ENGINE-GAP | Mutant accepts an oversized rebuild page. |
| REF-373 | conformance/reference/rebuild.go:113 | Rebuild request limit matches an existing session. | cursor-monotonicity | Mutant changes page size during a session. |
| REF-374 | conformance/reference/rebuild.go:116 | Rebuild sessions require current lineage and expiry. | cursor-monotonicity | Mutant resumes a stale rebuild session. |
| REF-375 | conformance/reference/rebuild.go:128 | Invalid rebuild pages return integrity failure. | checksum-convergence | Mutant emits an invalid rebuild page. |
| REF-376 | conformance/reference/rebuild.go:186 | Rebuild continuations bind to an existing session token. | cursor-monotonicity | Mutant accepts a continuation with mismatched session. |
| REF-377 | conformance/reference/rebuild.go:201 | Generated rebuild pages pass shape validation. | ENGINE-GAP | Mutant emits malformed page shape. |
| REF-378 | conformance/reference/rebuild.go:211 | Rebuild requests validate identity, UUID, limit, and token source. | ENGINE-GAP | Mutant accepts malformed rebuild input. |
| REF-379 | conformance/reference/rebuild.go:247 | Current rebuild sessions require staged or complete status, lineage, and expiry. | cursor-monotonicity | Mutant resumes expired or mismatched sessions. |
| REF-380 | conformance/reference/rebuild.go:270 | Rebuild staging rejects duplicate membership rows. | mutation-conservation | Mutant stages one row twice. |
| REF-381 | conformance/reference/rebuild.go:275 | Rebuild staging requires matching live versioned rows. | mutation-conservation | Mutant stages a missing, mismatched, or unversioned row. |
| REF-382 | conformance/reference/rebuild.go:291 | Rebuild staging requires a valid scope checksum. | checksum-convergence | Mutant stages rows with an invalid digest. |
| REF-383 | conformance/reference/rebuild.go:392 | Rebuild page ordinals advance exactly from the session cursor. | cursor-monotonicity | Mutant accepts a skipped or repeated page ordinal. |
| REF-384 | conformance/reference/rebuild.go:425 | Continuation token allocation cannot return zero. | cursor-monotonicity | Mutant stores a zero continuation token. |
| REF-385 | conformance/reference/rebuild.go:436 | Final cursor allocation cannot return zero. | cursor-monotonicity | Mutant stores a zero final cursor. |
| REF-386 | conformance/reference/rebuild.go:561 | Rebuild continuation validation rejects stale ordinal or session state. | cursor-monotonicity | Mutant resumes from stale continuation state. |
| REF-387 | conformance/reference/rebuild.go:597 | Rebuild records require complete identity and version. | mutation-conservation | Mutant emits incomplete rebuild records. |
| REF-388 | conformance/reference/rebuild.go:629 | Rebuild pages require ordinal and canonical content. | ENGINE-GAP | Mutant accepts an empty or zero-ordinal page. |
| REF-389 | conformance/reference/rebuild.go:633 | Continuation pages cannot carry final cursor or checksum. | cursor-monotonicity | Mutant marks a continuation page final. |
| REF-390 | conformance/reference/rebuild.go:635 | Final pages require final cursor and checksum. | checksum-convergence | Mutant marks a final page without finality data. |
| REF-391 | conformance/reference/rebuild.go:747 | Local rebuild pages match attempt phase, shape, and page size. | no-state-forks | Mutant applies a page invalid for the attempt. |
| REF-392 | conformance/reference/rebuild.go:752 | Stored rebuild page bytes equal canonical page bytes. | checksum-convergence | Mutant applies altered stored page content. |
| REF-393 | conformance/reference/rebuild.go:757 | Repeated page application requires equal page digest. | checksum-convergence | Mutant accepts changed repeated page content. |
| REF-394 | conformance/reference/rebuild.go:767 | Applied records match immutable staged records. | mutation-conservation | Mutant applies a changed rebuild row. |
| REF-395 | conformance/reference/rebuild.go:770 | Applied rebuild records are unique. | mutation-conservation | Mutant applies one staged ordinal twice. |
| REF-396 | conformance/reference/rebuild.go:850 | Local finalization requires pending finality and a nonzero final cursor. | cursor-monotonicity | Mutant finalizes without a final result. |
| REF-397 | conformance/reference/rebuild.go:855 | Local finalization verifies staged checksum and cardinality. | checksum-convergence | Mutant finalizes an incomplete or mismatched stage. |
| REF-398 | conformance/reference/rebuild.go:868 | Local finalization requires a current final cursor. | cursor-monotonicity | Mutant installs a stale final cursor. |
| REF-399 | conformance/reference/resolved_operations_test.go:21 | Resolved input is rejected for connect. | oracle-internal | Mutant accepts a resolved source on connect. |
| REF-400 | conformance/reference/resolved_operations_test.go:55 | Resolved input is defensively cloned before handler execution. | oracle-internal | Mutant lets a handler mutate caller-owned input. |
| REF-401 | conformance/reference/clock.go:54 | Restored token reservations reject zero handles. | oracle-internal | Mutant restores a zero token handle. |
| REF-402 | conformance/reference/clock.go:57 | Restored token reservations reject unsupported token kinds. | oracle-internal | Mutant restores an unsupported token kind. |
| REF-403 | conformance/reference/clock.go:64 | Conflicting token reservations are rejected. | oracle-internal | Mutant overwrites a conflicting reservation. |
| REF-404 | conformance/reference/clock.go:101 | Unsupported token kinds cannot mint tokens. | oracle-internal | Mutant mints an unsupported token kind. |
| REF-405 | conformance/reference/clock.go:105 | Token sequence allocation stops at the protocol limit. | oracle-internal | Mutant wraps token sequence allocation. |
| REF-406 | conformance/reference/clock.go:113 | Minted bindings are canonicalized before storage. | oracle-internal | Mutant stores noncanonical binding state. |
| REF-407 | conformance/reference/clock.go:123 | Unknown tokens validate as forged. | oracle-internal | Mutant accepts an unissued token. |
| REF-408 | conformance/reference/clock.go:125 | Token kind mismatch is distinct from forgery. | oracle-internal | Mutant accepts a token under another kind. |
| REF-409 | conformance/reference/clock.go:132 | Request binding mismatch is classified as misbound. | oracle-internal | Mutant classifies another client or scope as valid. |
| REF-410 | conformance/reference/clock.go:135 | Stale binding mismatch is classified as stale. | oracle-internal | Mutant classifies stale lineage as valid. |
| REF-411 | conformance/reference/connect.go:62 | Connect rejects unsupported protocol or runtime versions. | ENGINE-GAP | Mutant accepts an unsupported connect envelope. |
| REF-412 | conformance/reference/connect.go:66 | Connect rejects retired clients. | ENGINE-GAP | Mutant reconnects a retired client. |
| REF-413 | conformance/reference/connect.go:76 | Fresh connect rejects durable identity state. | no-state-forks | Mutant treats an existing client as fresh. |
| REF-414 | conformance/reference/connect.go:84 | Connect rejects a local schema that differs from the request. | ENGINE-GAP | Mutant accepts a schema lineage mismatch. |
| REF-415 | conformance/reference/connect.go:89 | Connect requires the current immutable schema. | ENGINE-GAP | Mutant connects without authoritative schema state. |
| REF-416 | conformance/reference/connect.go:98 | Fresh connect allocates no caller-supplied generation. | cursor-monotonicity | Mutant accepts a generation on fresh connect. |
| REF-417 | conformance/reference/connect.go:106 | Existing connect requires the current client generation. | cursor-monotonicity | Mutant accepts a stale client generation. |
| REF-418 | conformance/reference/connect.go:110 | Expired generations renew before further connect processing. | cursor-monotonicity | Mutant continues with an expired generation. |
| REF-419 | conformance/reference/connect.go:129 | Client scope-set version cannot exceed authoritative state. | cursor-monotonicity | Mutant accepts a future scope-set version. |
| REF-420 | conformance/reference/connect.go:132 | Known scopes must match local state. | scope-isolation | Mutant accepts a mismatched known-scope declaration. |
| REF-421 | conformance/reference/connect.go:153 | Unsupported schema lineage records a typed local error. | ENGINE-GAP | Mutant reports unsupported lineage as success without error state. |
| REF-422 | conformance/reference/connect.go:182 | Connect removes unassigned seed scopes. | scope-isolation | Mutant retains a removed seed scope. |
| REF-423 | conformance/reference/connect.go:188 | Schema transitions persist journal and rebuild intent. | no-state-forks | Mutant reports schema success without durable transition state. |
| REF-424 | conformance/reference/connect.go:197 | No-op schema connect requires a local schema. | ENGINE-GAP | Mutant completes a no-op connect without local schema. |
| REF-425 | conformance/reference/connect.go:211 | Connect reaches ready or rebuilding lifecycle according to pending intent. | ENGINE-GAP | Mutant selects the wrong final lifecycle. |
| REF-426 | conformance/reference/connect.go:242 | Connect envelope requires bounded runtime and scope-set values. | ENGINE-GAP | Mutant accepts missing or oversized envelope values. |
| REF-427 | conformance/reference/connect.go:258 | Connect envelope requires known scopes. | ENGINE-GAP | Mutant accepts a missing known-scope list. |
| REF-428 | conformance/reference/normalize.go:9 | State cloning copies every root map and mutable family. | oracle-internal | Mutant aliases a root state map. |
| REF-429 | conformance/reference/normalize.go:34 | Cloned state initializes every writable root map. | oracle-internal | Mutant leaves a nil clone map where writes are valid. |
| REF-430 | conformance/reference/normalize.go:78 | Root-map cloning preserves nil input and clones each value. | oracle-internal | Mutant aliases or changes nil-map semantics. |
| REF-431 | conformance/reference/normalize.go:89 | Byte slices are copied without changing nil state. | oracle-internal | Mutant aliases byte content. |
| REF-432 | conformance/reference/normalize.go:106 | Time pointers are copied independently. | oracle-internal | Mutant aliases mutable time pointers. |
| REF-433 | conformance/reference/normalize.go:114 | Snapshot times normalize to UTC without changing source state. | oracle-internal | Mutant preserves noncanonical snapshot time. |
| REF-434 | conformance/reference/normalize.go:122 | Schema manifests clone body, parent, tables, and affected scopes. | oracle-internal | Mutant returns an aliased schema manifest. |
| REF-435 | conformance/reference/normalize.go:173 | Registry clones nested relations, dependencies, rules, and impacts. | oracle-internal | Mutant aliases nested registry data. |
| REF-436 | conformance/reference/normalize.go:263 | Client state clones generations, retirement, assignments, and checkpoints. | oracle-internal | Mutant aliases client durable state. |
| REF-437 | conformance/reference/normalize.go:407 | Scope state cloning preserves effect and checksum isolation. | oracle-internal | Mutant aliases scope effects. |
| REF-438 | conformance/reference/normalize.go:617 | Canonical row cloning preserves fields and deletion metadata independently. | oracle-internal | Mutant aliases authoritative row fields. |
| REF-439 | conformance/reference/normalize.go:896 | Snapshot normalization sorts map entries deterministically. | oracle-internal | Mutant exposes nondeterministic map order. |
| REF-440 | conformance/reference/normalize.go:1040 | Snapshot normalization copies event and token data. | oracle-internal | Mutant returns a snapshot with aliased mutable data. |
| REF-441 | conformance/reference/normalize.go:1241 | Scope effect comparison includes source, operation, row, version, and projection. | oracle-internal | Mutant treats different effects as equal. |
| REF-442 | conformance/reference/normalize.go:1457 | Binding canonicalization normalizes all optional binding fields. | oracle-internal | Mutant produces unequal bindings for equivalent inputs. |
| REF-443 | conformance/reference/retention.go:40 | Expire-generation requires an existing nonretired client. | ENGINE-GAP | Mutant expires an unknown or retired client. |
| REF-444 | conformance/reference/retention.go:82 | Compaction batch size is positive and bounded. | ENGINE-GAP | Mutant accepts zero or oversized compaction. |
| REF-445 | conformance/reference/retention.go:90 | Compaction requires an authoritative scope. | scope-isolation | Mutant compacts an unknown scope. |
| REF-446 | conformance/reference/retention.go:94 | Compaction requires complete scope lineage. | cursor-monotonicity | Mutant compacts incomplete lineage. |
| REF-447 | conformance/reference/retention.go:100 | Existing retention floors must match current scope lineage. | cursor-monotonicity | Mutant reuses an obsolete floor. |
| REF-448 | conformance/reference/retention.go:112 | Active rebuild pins lower the safe compaction position. | cursor-monotonicity | Mutant compacts beyond a rebuild pin. |
| REF-449 | conformance/reference/retention.go:117 | No checkpoint falls back to the scope high watermark. | cursor-monotonicity | Mutant uses an unbounded or zero fallback. |
| REF-450 | conformance/reference/retention.go:119 | Safe position must use the current stream generation. | cursor-monotonicity | Mutant compacts across stream generations. |
| REF-451 | conformance/reference/retention.go:122 | Retention floors never move backward. | cursor-monotonicity | Mutant lowers an existing retention floor. |
| REF-452 | conformance/reference/retention.go:137 | Scope effects must use the current stream generation. | mutation-conservation | Mutant deletes effects from another stream. |
| REF-453 | conformance/reference/retention.go:148 | Compaction deletion respects the requested batch size. | mutation-conservation | Mutant deletes beyond the batch bound. |
| REF-454 | conformance/reference/retention.go:166 | Effect deletion and floor update commit together. | no-state-forks | Mutant commits only one half of compaction. |
| REF-455 | conformance/reference/schema.go:84 | Published schemas reject fresh references. | ENGINE-GAP | Mutant publishes a fresh schema reference. |
| REF-456 | conformance/reference/schema.go:95 | Class 3 publication requires affected scopes. | scope-isolation | Mutant publishes class 3 without affected scopes. |
| REF-457 | conformance/reference/schema.go:98 | Non-class-3 publication cannot declare affected scopes. | scope-isolation | Mutant attaches affected scopes to another class. |
| REF-458 | conformance/reference/schema.go:101 | Schema references are immutable. | no-state-forks | Mutant overwrites a published schema. |
| REF-459 | conformance/reference/schema.go:104 | Schema versions cannot reuse another hash. | cursor-monotonicity | Mutant publishes two hashes at one version. |
| REF-460 | conformance/reference/schema.go:115 | The first schema must be initial with matching compatibility floor. | ENGINE-GAP | Mutant accepts an invalid initial schema. |
| REF-461 | conformance/reference/schema.go:129 | Later schema versions increase monotonically. | cursor-monotonicity | Mutant publishes a stale schema version. |
| REF-462 | conformance/reference/schema.go:135 | Class 2 retains compatibility floor. | ENGINE-GAP | Mutant resets the floor for class 2. |
| REF-463 | conformance/reference/schema.go:139 | Class 3 and class 4 reset compatibility floor. | ENGINE-GAP | Mutant retains the floor for a reset class. |
| REF-464 | conformance/reference/schema.go:148 | Class 3 affected scopes pass authoritative validation. | scope-isolation | Mutant invalidates an unknown or duplicate scope. |
| REF-465 | conformance/reference/schema.go:181 | Client assignments require a declared assignment list. | scope-isolation | Mutant treats missing assignments as empty. |
| REF-466 | conformance/reference/schema.go:187 | Client assignments reject missing, duplicate, and unknown scopes. | scope-isolation | Mutant accepts an invalid assignment entry. |
| REF-467 | conformance/reference/schema.go:201 | Retired clients cannot receive assignments. | ENGINE-GAP | Mutant assigns scopes to a retired client. |
| REF-468 | conformance/reference/schema.go:210 | Assigned scopes require positive membership and retention generations. | cursor-monotonicity | Mutant assigns incomplete scope lineage. |
| REF-469 | conformance/reference/schema.go:229 | Assignment changes advance scope-set version once. | cursor-monotonicity | Mutant advances or preserves the version incorrectly. |
| REF-470 | conformance/reference/schema.go:237 | Assignment changes retain only current assignment checkpoints. | scope-isolation | Mutant retains checkpoints for removed scopes. |
| REF-471 | conformance/reference/schema.go:252 | Schema references require bounded version and hash fields. | ENGINE-GAP | Mutant accepts an invalid schema reference. |
| REF-472 | conformance/reference/schema.go:284 | Published manifests require complete table identity and composition. | ENGINE-GAP | Mutant accepts an incomplete table manifest. |
| REF-473 | conformance/reference/schema.go:323 | Published fields require unique IDs and names. | ENGINE-GAP | Mutant accepts duplicate field identity. |
| REF-474 | conformance/reference/schema.go:376 | Primary key and timestamp field references must resolve to declared fields. | ENGINE-GAP | Mutant accepts missing manifest field references. |
| REF-475 | conformance/reference/schema.go:425 | Index fields must resolve and preserve declared uniqueness. | ENGINE-GAP | Mutant accepts an invalid index manifest. |
| REF-476 | conformance/reference/schema.go:467 | Class 3 scope invalidation changes only affected authoritative assignments. | scope-isolation | Mutant invalidates an unaffected scope. |
| REF-477 | conformance/reference/schema.go:507 | Schema action maps are closed over supported classes. | ENGINE-GAP | Mutant produces an undefined schema action. |
| REF-478 | conformance/reference/seed.go:38 | Portable seed payload uses strict decoding. | ENGINE-GAP | Mutant accepts malformed seed payload. |
| REF-479 | conformance/reference/seed.go:41 | Portable seed request identity and artifact IDs are exact. | no-state-forks | Mutant installs a seed under another identity. |
| REF-480 | conformance/reference/seed.go:44 | Portable seed requires only resolved seed input. | oracle-internal | Mutant accepts an unrelated source step. |
| REF-481 | conformance/reference/seed.go:48 | Fixture identity must match the request. | no-state-forks | Mutant accepts a misbound fixture. |
| REF-482 | conformance/reference/seed.go:51 | Portable seed artifact and manifest digests verify before installation. | checksum-convergence | Mutant installs tampered seed bytes. |
| REF-483 | conformance/reference/seed.go:56 | Portable seed requires both local and server client state. | ENGINE-GAP | Mutant installs a seed for a missing client. |
| REF-484 | conformance/reference/seed.go:64 | Portable seed target state must be eligible. | scope-isolation | Mutant installs into an ineligible local target. |
| REF-485 | conformance/reference/seed.go:67 | Receipt authority exhaustion prevents seed mutation. | no-state-forks | Mutant installs rows without a receipt. |
| REF-486 | conformance/reference/seed.go:71 | Installed rows preserve authoritative identity, fields, versions, and checksums. | mutation-conservation | Mutant changes seeded row content. |
| REF-487 | conformance/reference/seed.go:88 | Seed receipts bind to fixture scope, boundary, and issuance time. | checksum-convergence | Mutant mints an unbound seed receipt. |
| REF-488 | conformance/reference/seed.go:115 | Artifact digest validation rejects empty or mismatched artifacts. | checksum-convergence | Mutant accepts a bad artifact digest. |
| REF-489 | conformance/reference/seed.go:122 | Export lineage requires a canonical UUID. | ENGINE-GAP | Mutant accepts malformed export lineage. |
| REF-490 | conformance/reference/seed.go:125 | Seed schema and registry lineage must match current authority. | scope-isolation | Mutant installs an obsolete contract. |
| REF-491 | conformance/reference/seed.go:131 | Seed registry generation must be active and validated exactly once. | no-state-forks | Mutant accepts inactive or duplicate registry authority. |
| REF-492 | conformance/reference/seed.go:134 | Seed snapshot boundary must be current-generation and materialized. | cursor-monotonicity | Mutant installs from an unmaterialized boundary. |
| REF-493 | conformance/reference/seed.go:178 | Portable seed declares exactly one matching scope. | scope-isolation | Mutant installs multiple or mismatched scopes. |
| REF-494 | conformance/reference/seed.go:182 | Portable scope declarations are sorted and unique. | scope-isolation | Mutant accepts duplicate or unsorted scope declarations. |
| REF-495 | conformance/reference/seed.go:188 | Seed scope generations must match authoritative scope generations. | cursor-monotonicity | Mutant installs stale scope lineage. |
| REF-496 | conformance/reference/seed.go:192 | Seed scope cardinality equals the portable seed contract. | mutation-conservation | Mutant installs a partial seed. |
| REF-497 | conformance/reference/seed.go:198 | Seed row count equals the portable seed contract. | mutation-conservation | Mutant installs an incomplete row set. |
| REF-498 | conformance/reference/seed.go:211 | Seed rows sort by ordinal before validation. | ENGINE-GAP | Mutant validates source order instead of canonical ordinal order. |
| REF-499 | conformance/reference/seed.go:214 | Seed row ordinals are contiguous and scope-bound. | mutation-conservation | Mutant accepts a skipped, repeated, or foreign ordinal. |
| REF-500 | conformance/reference/seed.go:219 | Seed rows reject deleted or incomplete authoritative rows. | mutation-conservation | Mutant installs tombstones or incomplete rows. |
| REF-501 | conformance/reference/seed.go:236 | Seed row checksums match the schema manifest. | checksum-convergence | Mutant accepts a row with a mismatched checksum. |
| REF-502 | conformance/reference/seed.go:251 | Seed row checksums aggregate to the declared scope checksum. | checksum-convergence | Mutant accepts a mismatched scope digest. |
| REF-503 | conformance/reference/seed.go:273 | Seed fixture verification is all-or-nothing. | no-state-forks | Mutant installs verified rows before a later row fails. |
| REF-504 | conformance/reference/seed_connect_test.go:35 | Seed install requires a resolved portable seed fixture. | oracle-internal | Mutant accepts missing or unrelated resolved input. |
| REF-505 | conformance/reference/seed_connect_test.go:53 | Seed install rejects tampered artifact bytes. | checksum-convergence | Mutant accepts altered artifact content. |
| REF-506 | conformance/reference/seed_connect_test.go:72 | Seed install rejects changed scope generations. | cursor-monotonicity | Mutant accepts stale scope lineage. |
| REF-507 | conformance/reference/seed_connect_test.go:98 | Seed install writes exactly the fixture rows and receipt. | mutation-conservation | Mutant drops, duplicates, or alters seeded rows. |
| REF-508 | conformance/reference/seed_connect_test.go:47 | Seed receipt binds to the captured export and boundary. | checksum-convergence | Mutant creates an unbound receipt. |
| REF-509 | conformance/reference/seed_connect_test.go:28 | Seed installation does not create pull-visible server effects. | mutation-conservation | Mutant adds server effects for local seed installation. |
| REF-510 | conformance/reference/state_test.go:305 | State snapshots include every state family. | oracle-internal | Mutant omits a state family from a snapshot. |
| REF-511 | conformance/reference/state_test.go:78 | State snapshots use deterministic ordering. | oracle-internal | Mutant exposes map iteration order. |
| REF-512 | conformance/reference/state_test.go:443 | Snapshot mutation does not alter model state. | oracle-internal | Mutant returns aliased nested state. |
| REF-513 | conformance/reference/state_test.go:46 | Equal states produce equal normalized snapshots. | oracle-internal | Mutant preserves irrelevant map ordering. |
| REF-514 | conformance/reference/state_test.go:465 | Clone state preserves nil versus empty collection semantics. | oracle-internal | Mutant changes collection presence during cloning. |
| REF-515 | conformance/reference/state_test.go:428 | Clone state isolates nested row, scope, client, and stream values. | oracle-internal | Mutant aliases nested mutable values. |
| REF-516 | conformance/reference/types.go:142 | Protocol counters reject zero and overflow values where required. | cursor-monotonicity | Mutant accepts an invalid protocol counter. |
| REF-517 | conformance/reference/types.go:183 | Canonical UUID validation rejects malformed identities. | ENGINE-GAP | Mutant accepts noncanonical UUID input. |
| REF-518 | conformance/reference/types.go:227 | Canonical wire JSON rejects noncanonical values. | checksum-convergence | Mutant accepts semantically equivalent but noncanonical wire data. |
| REF-519 | conformance/reference/types.go:269 | Field values preserve declared portable types. | checksum-convergence | Mutant accepts a field with the wrong portable type. |
| REF-520 | conformance/reference/types.go:318 | Schema composition values are closed. | ENGINE-GAP | Mutant accepts an unknown composition. |
| REF-521 | conformance/reference/types.go:361 | Registration kinds are closed. | ENGINE-GAP | Mutant accepts an unknown registration kind. |
| REF-522 | conformance/reference/types.go:409 | Effect operations are closed. | ENGINE-GAP | Mutant accepts an unknown effect operation. |
| REF-523 | conformance/reference/types.go:458 | Cursor dispositions are closed. | cursor-monotonicity | Mutant accepts an unknown cursor disposition. |
| REF-524 | conformance/reference/types.go:493 | Token kinds are closed. | ENGINE-GAP | Mutant accepts an unknown token kind. |
| REF-525 | conformance/reference/types.go:546 | Reason codes remain bounded and serializable. | ENGINE-GAP | Mutant accepts an unknown reason code. |
| REF-526 | conformance/reference/types.go:612 | Binding sets preserve presence flags and canonical optional values. | no-state-forks | Mutant conflates absent and empty bindings. |
| REF-527 | conformance/reference/types.go:684 | Stream positions compare generation, commit, event, and effect order. | cursor-monotonicity | Mutant compares only one position component. |
| REF-528 | conformance/reference/types.go:742 | Row identities compare canonical identity bytes and table identity. | mutation-conservation | Mutant conflates distinct row identities. |
| REF-529 | conformance/reference/types.go:801 | Checksums use canonical row and scope material. | checksum-convergence | Mutant computes a digest from noncanonical material. |
| REF-530 | conformance/reference/types.go:885 | Client and scope assignment keys preserve identity boundaries. | scope-isolation | Mutant conflates assignments across clients or scopes. |
| REF-531 | conformance/reference/types.go:941 | Error status and retry classes remain closed. | ENGINE-GAP | Mutant accepts an undefined error class. |
| REF-532 | conformance/reference/types.go:1007 | Observation result kinds remain mutually exclusive. | oracle-internal | Mutant emits incompatible result observations. |
| REF-533 | conformance/reference/types.go:1071 | WAL replay keys preserve stream and commit identity. | mutation-conservation | Mutant conflates transactions across stream generations. |
| REF-534 | conformance/reference/types.go:1138 | Rebuild keys preserve client, generation, scope, and session identity. | scope-isolation | Mutant conflates rebuild sessions. |
| REF-535 | conformance/reference/types.go:1202 | Local provenance preserves row-to-scope ownership. | scope-isolation | Mutant applies a row under an unrelated scope. |
| REF-536 | conformance/reference/wal.go:196 | Source transaction payload decoding is strict and context-aware. | ENGINE-GAP | Mutant accepts malformed or canceled source commit input. |
| REF-537 | conformance/reference/wal.go:205 | WAL transaction keys require valid stream and commit identities. | cursor-monotonicity | Mutant accepts an invalid transaction key. |
| REF-538 | conformance/reference/wal.go:218 | Source transaction replay requires identical committed content. | mutation-conservation | Mutant accepts replay with changed content. |
| REF-539 | conformance/reference/wal.go:224 | WAL transaction end positions advance in valid order. | cursor-monotonicity | Mutant accepts an end position before a prior transaction. |
| REF-540 | conformance/reference/wal.go:230 | Source commit time and event capture time are assigned consistently. | ENGINE-GAP | Mutant assigns inconsistent event timestamps. |
| REF-541 | conformance/reference/wal.go:235 | Committed fences and live source rows update with the transaction. | no-state-forks | Mutant commits only part of source transaction state. |
| REF-542 | conformance/reference/wal.go:243 | Materialization validates bounded failure classes. | ENGINE-GAP | Mutant accepts an unbounded WAL failure class. |
| REF-543 | conformance/reference/wal.go:269 | Repair requires active poison for the same transaction. | mutation-conservation | Mutant repairs an unrelated or unpoisoned transaction. |
| REF-544 | conformance/reference/wal.go:273 | Truncate poison requires authorized stream reset. | ENGINE-GAP | Mutant repairs truncate poison through normal retry. |
| REF-545 | conformance/reference/wal.go:279 | Materialization stops on canceled context. | no-state-forks | Mutant commits materialization after cancellation. |
| REF-546 | conformance/reference/wal.go:290 | Completed transactions are idempotent and cannot be repaired. | mutation-conservation | Mutant reprocesses completed materialization. |
| REF-547 | conformance/reference/wal.go:297 | WAL materialization requires the next transaction in order. | cursor-monotonicity | Mutant materializes transactions out of order. |
| REF-548 | conformance/reference/wal.go:742 | WAL source events require valid before and after images by operation. | mutation-conservation | Mutant accepts an invalid image shape. |
| REF-549 | conformance/reference/wal.go:861 | WAL registered identities require exactly one registration variant. | scope-isolation | Mutant accepts both synced and capture identity. |
| REF-550 | conformance/reference/wal.go:1025 | WAL projections bind to captured source transaction and row version. | checksum-convergence | Mutant accepts a projection from another source. |
| REF-551 | conformance/reference/wal.go:1157 | WAL scope evaluation rejects unknown scopes and overfanout. | scope-isolation | Mutant emits unbounded or unknown scope effects. |
| REF-552 | conformance/reference/wal.go:1268 | WAL materialization preserves effect ordering by stream position. | cursor-monotonicity | Mutant reorders materialized effects. |
| REF-553 | conformance/reference/wal.go:1364 | WAL failures create bounded poison records and no partial effects. | no-state-forks | Mutant leaves partial materialization beside poison. |
| REF-554 | conformance/reference/wal.go:1455 | Contiguous-prefix acknowledgement advances only completed transactions. | cursor-monotonicity | Mutant acknowledges a gap or incomplete transaction. |
| REF-555 | conformance/reference/wal.go:1510 | WAL worker restart preserves stream authority and clears only transient worker state. | no-state-forks | Mutant resets durable WAL authority on restart. |
| REF-556 | conformance/reference/wal_operations_test.go:42 | Source commit rejects malformed transaction identity. WITHDRAWN: The described malformed-transaction-identity check does not exist in the current source test. | WITHDRAWN | Mutant accepts malformed WAL transaction input. |
| REF-557 | conformance/reference/wal_operations_test.go:75 | Source commit is idempotent for identical replay. WITHDRAWN: The described identical source-replay idempotency check does not exist in the current source test. | WITHDRAWN | Mutant duplicates effects or fences on replay. |
| REF-558 | conformance/reference/wal_operations_test.go:101 | Changed replay content returns integrity failure without mutation. WITHDRAWN: The described changed-replay integrity check does not exist in the current source test. | WITHDRAWN | Mutant mutates state on conflicting replay. |
| REF-559 | conformance/reference/wal_operations_test.go:127 | Materialization failure records poison and quarantines dependent work. | mutation-conservation | Mutant exposes partial effects after failure. |
| REF-560 | conformance/reference/wal_operations_test.go:168 | Contiguous acknowledgement does not skip a failed transaction. | cursor-monotonicity | Mutant advances acknowledgement over a poison gap. |
| REF-A01 | conformance/reference/state_test.go:47 | Root-map insertion order does not change a state snapshot. | oracle-internal | Reverse root-map insertion order. |
| REF-A02 | conformance/reference/state_test.go:78 | Snapshot schema, client, and row keys use their required deterministic ordering. | oracle-internal | Swap one comparator branch. |
| REF-A03 | conformance/reference/state_test.go:110 | Stream-position comparison orders generation start, effects, transaction end, and unknown kinds. | oracle-internal | Reverse transaction-end ordering. |
| REF-A04 | conformance/reference/state_test.go:134 | Row comparison breaks equal canonical-byte ties with the remaining identity fields. | oracle-internal | Remove the secondary identity comparison. |
| REF-A05 | conformance/reference/state_test.go:144 | Snapshot normalization preserves semantic order for stream, queue, rebuild, journal, event, and seed sequences. | oracle-internal | Sort one semantic sequence. |
| REF-A06 | conformance/reference/state_test.go:217 | Snapshots retain projection images, transaction poison, and reset-fence authority. | oracle-internal | Omit one image or fence authority field. |
| REF-A07 | conformance/reference/state_test.go:248 | Snapshots retain and normalize active candidate projection stages. | oracle-internal | Omit a candidate row, projection, fence, or scope. |
| REF-A08 | conformance/reference/state_test.go:280 | Inactive candidate stages remain cloned and snapshot-isolated. | oracle-internal | Alias inactive candidate-stage row data. |
| REF-A09 | conformance/reference/state_test.go:305 | Snapshots retain expanded registry, client, seed, installation, readiness, and rebuild state. | oracle-internal | Omit one expanded durable state family. |
| REF-A10 | conformance/reference/state_test.go:365 | Effect-operation ranks retain the closed delete-before-upsert order. | oracle-internal | Swap the two operation ranks. |
| REF-A11 | conformance/reference/state_test.go:377 | Snapshots retain source rows, replay, poison, local queue, and rebuild replay state. | oracle-internal | Omit one nested Task 6 state field. |
| REF-A12 | conformance/reference/state_test.go:511 | State has no retired identity root and StateSnapshot contains no map. | oracle-internal | Add a map field to StateSnapshot. |
| REF-A13 | conformance/reference/resolved_operations_test.go:72 | A resolved-handler error rolls back model state and token allocation. | oracle-internal | Commit a token before returning the handler error. |
| REF-A14 | conformance/reference/resolved_operations_test.go:85 | Local pull apply writes the captured projection, provenance, and verified checkpoint atomically. | mutation-conservation | Apply a projection without its matching local row or provenance. |
| REF-A15 | conformance/reference/resolved_operations_test.go:135 | A misbound or changed pull source rejects without server or local mutation. | no-state-forks | Commit local state after a source mismatch. |
| REF-A16 | conformance/reference/resolved_operations_test.go:159 | Portable-seed installation has no server authority or checkpoint grant. | scope-isolation | Add a server checkpoint during seed installation. |
| REF-A17 | conformance/reference/resolved_operations_test.go:215 | Invalid portable-seed fixtures reject without state or receipt-token mutation. | no-state-forks | Advance the receipt authority before rejecting a fixture. |
| REF-A18 | conformance/reference/seed_connect_test.go:62 | A stale assigned seed receipt becomes rebuild-required while seed rows remain local. | scope-isolation | Retain a runnable cursor for the stale receipt. |
| REF-A19 | conformance/reference/seed_connect_test.go:77 | An unassigned seed receipt removes only its local seed scope and data. | scope-isolation | Retain the unassigned local seed scope. |
| REF-A20 | conformance/reference/wal_operations_test.go:67 | Source transactions retain commit order, selected registry generation, and original event ordinal. | cursor-monotonicity | Sort by arrival order or renumber an event ordinal. |
| REF-A21 | conformance/reference/wal_operations_test.go:121 | A WAL materialization fault creates poison without partial rows, projections, effects, or fences. | no-state-forks | Commit one projection before creating poison. |
| REF-A22 | conformance/reference/wal_operations_test.go:167 | Acknowledgement stops at poison and resumes only after contiguous repair. | cursor-monotonicity | Advance acknowledgement over the poison gap. |
| REF-A23 | conformance/reference/wal_operations_test.go:195 | Worker restart preserves poison and materialized state, then repair unblocks later work. | no-state-forks | Clear poison during worker restart. |
| REF-A24 | conformance/reference/wal_operations_test.go:221 | A truncate event creates blocking poison without partial materialization state. | no-state-forks | Persist a fence or projection for truncate. |
| REF-A25 | conformance/reference/wal_operations_test.go:241 | Repeated row events retain only the greatest causal scope effect and cover every fence. | mutation-conservation | Emit both effects for the same scoped row. |
| REF-A26 | conformance/reference/wal_operations_test.go:279 | Dependency changes enter and leave scopes without changing unrelated row versions. | scope-isolation | Change the synchronized row during dependency-only work. |
| REF-A27 | conformance/reference/wal_operations_test.go:312 | Multi-scope materialization keeps effects and checksums independent per scope. | checksum-convergence | Reuse one scope checksum for both scopes. |
| REF-A28 | conformance/reference/wal_operations_test.go:373 | Membership staging is invisible before activation and changes only affected scope state. | scope-isolation | Expose a staged membership change before activation. |
| REF-A29 | conformance/reference/wal_operations_test.go:459 | Membership administrative limits reject atomically and valid staged batches retain their cardinality. | ENGINE-GAP | Accept an over-limit staging request. |
| REF-A30 | conformance/reference/wal_operations_test.go:525 | Invalid WAL and membership operation payloads do not mutate the reference model. | oracle-internal | Commit state after accepting an unknown payload member. |
| REF-A31 | conformance/reference/wal_operations_test.go:571 | The temporary WAL-oracle negative controls detect every listed mutant. | oracle-internal | Remove one violation detector. |
| REF-A32 | conformance/reference/push_operations_test.go:394 | Push CAS outcomes partition accepted and rejected mutations with exact status and reason codes. | mutation-conservation | Return a conflicting mutation as accepted. |
| REF-A33 | conformance/reference/push_operations_test.go:436 | Soft and hard deletes retain required source images and nonresurrection fence evidence. | mutation-conservation | Permit an insert after accepted deletion. |
| REF-A34 | conformance/reference/push_operations_test.go:535 | Mixed push outcomes retain typed outcome order, canonical evidence, and ledger state. | mutation-conservation | Drop one rejected outcome from the ledger. |
| REF-A35 | conformance/reference/push_operations_test.go:634 | Malformed push shapes and duplicate mutation IDs reject without durable mutation. | no-state-forks | Create a batch ledger before rejecting the request. |
| REF-A36 | conformance/reference/push_operations_test.go:665 | Completed replay preserves server time and ledger state after authority changes. | mutation-conservation | Recompute server time on replay. |
| REF-A37 | conformance/reference/push_operations_test.go:742 | An expired generation rejects an unexecuted transport-failure retry without server ledgers. | cursor-monotonicity | Create a ledger for the expired retry. |
| REF-A38 | conformance/reference/push_operations_test.go:762 | Batch and mutation idempotency conflicts preserve state, while historical replay returns stored evidence without DML. | mutation-conservation | Execute DML for a historical mutation replay. |
| REF-A39 | conformance/reference/push_operations_test.go:824 | Response loss preserves the sealed request and reconciles exactly once from canonical replay. | mutation-conservation | Reconcile the same sealed batch twice. |
| REF-A40 | conformance/reference/push_operations_test.go:898 | Transport failure changes only local sealed-request and retry state. | no-state-forks | Persist source rows during transport failure. |
| REF-A41 | conformance/reference/pull_rebuild_operations_test.go:351 | Rebuild pages remain immutable snapshots and preserve unrelated scope progress until final acknowledgement. | no-state-forks | Rebuild from a changed live row. |
| REF-A42 | conformance/reference/pull_rebuild_operations_test.go:465 | Expired, epoch-invalidated, and forged rebuild continuations require restart without session mutation. | cursor-monotonicity | Accept a forged continuation token. |
| REF-A43 | conformance/reference/pull_rebuild_operations_test.go:550 | Local rebuild pruning preserves overlapping provenance and pending local intent. | scope-isolation | Delete a row still owned by another scope. |

## Consumers of `conformance/reference/`

| consumer | import source |
|---|---|
| `conformance/blackbox/syntheticproof/compare.go` | `conformance/blackbox/syntheticproof/compare.go:13` |
| `conformance/blackbox/syntheticproof/synthetic.go` | `conformance/blackbox/syntheticproof/synthetic.go:21` |
| `conformance/modelrunner/macro.go` | `conformance/modelrunner/macro.go:8` |
| `conformance/modelrunner/runner.go` | `conformance/modelrunner/runner.go:13` |
| `conformance/modelrunner/runner_test.go` | `conformance/modelrunner/runner_test.go:10` |
| `conformance/modelrunner/schema_dispatch.go` | `conformance/modelrunner/schema_dispatch.go:8` |
| `conformance/modelrunner/seed.go` | `conformance/modelrunner/seed.go:16` |
| `conformance/modelrunner/semantic.go` | `conformance/modelrunner/semantic.go:16` |
| `conformance/modelrunner/state_facts.go` | `conformance/modelrunner/state_facts.go:9` |
| `conformance/modelrunner/types.go` | `conformance/modelrunner/types.go:10` |
| `conformance/modelrunner/workload_cardinality.go` | `conformance/modelrunner/workload_cardinality.go:12` |
| `conformance/modelrunner/workload_cardinality_test.go` | `conformance/modelrunner/workload_cardinality_test.go:11` |
| `conformance/modelrunner/workload_configured_limits.go` | `conformance/modelrunner/workload_configured_limits.go:11` |
| `conformance/modelrunner/workload_queue_limits.go` | `conformance/modelrunner/workload_queue_limits.go:14` |
| `conformance/modelrunner/workload_queue_limits_test.go` | `conformance/modelrunner/workload_queue_limits_test.go:10` |
| `conformance/modelrunner/workload_topology.go` | `conformance/modelrunner/workload_topology.go:12` |
| `conformance/modelrunner/workload_topology_test.go` | `conformance/modelrunner/workload_topology_test.go:13` |

## Corrected anchors

| check-id | old anchor | new anchor |
|---|---|---|
| REF-510 | conformance/reference/state_test.go:32 | conformance/reference/state_test.go:305 |
| REF-511 | conformance/reference/state_test.go:78 | conformance/reference/state_test.go:46 |
| REF-512 | conformance/reference/state_test.go:112 | conformance/reference/state_test.go:443 |
| REF-513 | conformance/reference/state_test.go:159 | conformance/reference/state_test.go:46 |
| REF-514 | conformance/reference/state_test.go:208 | conformance/reference/state_test.go:465 |
| REF-515 | conformance/reference/state_test.go:251 | conformance/reference/state_test.go:428 |
| REF-556 | conformance/reference/wal_operations_test.go:42 | WITHDRAWN: The described malformed-transaction-identity check does not exist in the current source test. |
| REF-557 | conformance/reference/wal_operations_test.go:75 | WITHDRAWN: The described identical source-replay idempotency check does not exist in the current source test. |
| REF-558 | conformance/reference/wal_operations_test.go:101 | WITHDRAWN: The described changed-replay integrity check does not exist in the current source test. |
| REF-559 | conformance/reference/wal_operations_test.go:163 | conformance/reference/wal_operations_test.go:127 |
| REF-560 | conformance/reference/wal_operations_test.go:246 | conformance/reference/wal_operations_test.go:168 |
