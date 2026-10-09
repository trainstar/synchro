# Changelog

This file records the changes in each Synchro release that affect applications and operators.
The extension, the Go adapter, and the Swift, Kotlin, and React Native clients release together with one version.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Synchro uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
Before `0.4.0`, use the [GitHub Releases](https://github.com/trainstar/synchro/releases) notes.

## [Unreleased]

## [0.4.0-rc.2] - 2026-10-09

This release candidate is for consumer validation of `0.4.0`.
Use exact versions. See [Release Candidate](https://github.com/trainstar/synchro/blob/master/RELEASE.md#release-candidate) for registry behavior.

### Added

- Operators can register an assignment function with a SQL NULL bound to return all distinct assigned scopes. ([#300](https://github.com/trainstar/synchro/issues/300))

### Changed

- Swift Package Manager and CocoaPods integrations require GRDB 7.8 or later consistently for native inspection APIs.
- React Native applications can use RN 0.82.1 through 0.83.x. RN 0.82.1 iOS applications require the [fmt 12.1.0 backport](https://trainstar.github.io/synchro/reference/support-policy/#react-native-0821-fmt-backport) with Xcode 27. ([#303](https://github.com/trainstar/synchro/issues/303))
- Breaking: Apple clients require iOS 17 or later. Increase the application deployment target before upgrading. See [client requirements](https://trainstar.github.io/synchro/clients/consumption/). ([#303](https://github.com/trainstar/synchro/issues/303))

### Fixed

- Native clients reject inconsistent migration journals and retain unfinished scope rebuild work during schema changes. Native clients preserve pending local edits and deletes when compatible schema changes replace cursors. Native clients rebuild only the affected scope after a local checksum mismatch. ([#331](https://github.com/trainstar/synchro/issues/331))
- Swift clients apply remote changes after a later accepted or conflict outcome replaces a terminal rejection. ([#331](https://github.com/trainstar/synchro/issues/331))
- Kotlin clients preserve rows when fields become nullable and support required-field additions without invented defaults. ([#331](https://github.com/trainstar/synchro/issues/331))

## [0.4.0-rc.1] - 2026-10-01

This release candidate is for consumer validation of `0.4.0`.
Use exact versions. See [Release Candidate](https://github.com/trainstar/synchro/blob/master/RELEASE.md#release-candidate) for registry behavior.

### Before you update

Read [Before you update to 0.4.0](https://trainstar.github.io/synchro/operations/extension-update/#before-you-update-to-040) before you update a `0.3.x` server.
Some deployments must release new clients or repair registrations first.

### Added

- Atomic write transactions. `atomicWriteTransaction` on the Swift, Kotlin, and React Native `SynchroClient` captures one application write transaction as one mutation group. The server applies all synced mutations of the group or none of them. See [Atomic write transactions](https://trainstar.github.io/synchro/clients/application-sql/#atomic-write-transactions). ([#33](https://github.com/trainstar/synchro/issues/33))
- Commit-time validation of atomic groups. An invalid group rolls back the local transaction and raises `atomicGroupInvalid` with a typed reason. ([#33](https://github.com/trainstar/synchro/issues/33))
- The `atomic_batch_rejected` push outcome. Each member of a failed atomic group that did not cause the failure gets this terminal code. ([#33](https://github.com/trainstar/synchro/issues/33))
- Dynamic scope assignment. `synchro_register_assignment_function` registers one SQL function that derives the scopes of a user from application data. See [Assignment function](https://trainstar.github.io/synchro/architecture/scope-modeling/#assignment-function). ([#33](https://github.com/trainstar/synchro/issues/33))
- Pull-time assignment reconciliation. Each successful connect and pull applies grants, revocations, and assignment function results. A changed set advances `scope_set_version` and appears in `scope_updates`. ([#33](https://github.com/trainstar/synchro/issues/33), [#260](https://github.com/trainstar/synchro/issues/260))
- Key-only and default-only creates. An insert with an explicit empty column set is valid. ([#220](https://github.com/trainstar/synchro/issues/220))

### Changed

- Breaking: the server checks each deferrable constraint at the end of each push unit. A failed check rejects only that unit, not the complete push request. ([#250](https://github.com/trainstar/synchro/issues/250))
- Breaking: each accepted push outcome reports the row state after its push unit. An outcome without a row means that the row is absent. `0.3.x` clients cannot read that outcome. ([#268](https://github.com/trainstar/synchro/issues/268))
- Breaking: a registration with push policy `enabled` rejects a database-generated primary key. Clients cannot write an identity `ALWAYS` column. ([#249](https://github.com/trainstar/synchro/issues/249))
- Breaking: the extension evaluates each membership, impact, and assignment function as its owner in a security-restricted operation. The owner must be able to read each relation that the function reads. ([#247](https://github.com/trainstar/synchro/issues/247))
- Projection views bind to the relation OID. The update rebuilds the existing views. ([#247](https://github.com/trainstar/synchro/issues/247))
- The push transaction locks the source row with the lock mode of its statement, not always `FOR UPDATE`. ([#277](https://github.com/trainstar/synchro/issues/277))
- Push outcomes do not reveal whether a row that row security hides exists. ([#265](https://github.com/trainstar/synchro/issues/265), [#269](https://github.com/trainstar/synchro/issues/269))
- The idle WAL worker writes its progress less often. ([#233](https://github.com/trainstar/synchro/issues/233))
- The WAL worker applies a configuration reload after `SIGHUP`. ([#248](https://github.com/trainstar/synchro/issues/248))
- Decode poison states its actual cause. ([#232](https://github.com/trainstar/synchro/issues/232))

### Fixed

Server:

- Push failed when an application trigger wrote the pushed row again. ([#234](https://github.com/trainstar/synchro/issues/234))
- Accepted registered relation shapes failed later capture and snapshot operations. ([#211](https://github.com/trainstar/synchro/issues/211))
- Push returned `500` for an unknown non-UUID table ID. ([#227](https://github.com/trainstar/synchro/issues/227))
- Readiness was false while a healthy WAL worker processed writes. ([#239](https://github.com/trainstar/synchro/issues/239))
- The WAL worker did not acknowledge WAL without a published change, so idle readiness failed. ([#231](https://github.com/trainstar/synchro/issues/231))
- Two registrations that committed before activation poisoned the stream. ([#242](https://github.com/trainstar/synchro/issues/242))
- Batched registry activations after an extension reinstall poisoned the stream. ([#256](https://github.com/trainstar/synchro/issues/256))
- Registry work could deadlock when a projection view was prepared during registration. ([#263](https://github.com/trainstar/synchro/issues/263))
- Pull failed for a scope after a parent change removed dependent children. ([#241](https://github.com/trainstar/synchro/issues/241))
- Same-table membership dependencies did not propagate sibling changes. ([#215](https://github.com/trainstar/synchro/issues/215))
- Snapshot activation could replace current versions and reject valid row histories. ([#198](https://github.com/trainstar/synchro/issues/198))
- Schema transitions could replace source values with null and skip unrelated drift checks. ([#200](https://github.com/trainstar/synchro/issues/200))
- A rejected connect request could change durable client state. ([#207](https://github.com/trainstar/synchro/issues/207))
- Worker startup without a bound slot could delete an inactive slot. ([#203](https://github.com/trainstar/synchro/issues/203))
- Portable seed verification omitted field domains. ([#208](https://github.com/trainstar/synchro/issues/208))

Clients:

- Native clients did not push changes captured before start when startup resumed a pull or rebuild backoff. ([#240](https://github.com/trainstar/synchro/issues/240))
- Native clients could send a normalized parent mutation after its child mutation. ([#243](https://github.com/trainstar/synchro/issues/243))
- Native clients did not block a mutation that depends on a blocked predecessor. ([#246](https://github.com/trainstar/synchro/issues/246))
- A Class 4 `rebuild_local` reset dropped the local row of a retained blocked mutation. ([#267](https://github.com/trainstar/synchro/issues/267))
- Clients kept a local row after an atomic rollback conflict with no server row. ([#259](https://github.com/trainstar/synchro/issues/259))
- Swift: ordinary trigger updates could change synced rows without retained intent. ([#219](https://github.com/trainstar/synchro/issues/219))
- Swift: public reads restarted the push debounce. Automatic push could wait until the sync interval. ([#245](https://github.com/trainstar/synchro/issues/245))
- Kotlin: the client sent an insert and delete pair that normalization must cancel. ([#261](https://github.com/trainstar/synchro/issues/261))
- Kotlin: numeric equality and hashing disagreed at precision boundaries. ([#204](https://github.com/trainstar/synchro/issues/204))
- Kotlin: cancellation and callback shutdown could leave operation ownership undrained. ([#223](https://github.com/trainstar/synchro/issues/223))
- Accepted legacy intent could not be inspected without modern bindings. ([#224](https://github.com/trainstar/synchro/issues/224))
- React Native: native bridge transaction failures could leave promises unsettled. ([#199](https://github.com/trainstar/synchro/issues/199))

### Security

- Extension security definer functions search the temporary schema last. An unqualified name cannot resolve to a caller temporary relation. ([#258](https://github.com/trainstar/synchro/issues/258))
- Membership backfill does not use a caller temporary table. ([#262](https://github.com/trainstar/synchro/issues/262))
- Registered scope functions do not run with extension role privileges. ([#247](https://github.com/trainstar/synchro/issues/247))
- Parser and rebuild diagnostics do not include submitted or record-owned values. ([#210](https://github.com/trainstar/synchro/issues/210))
- The React Native lock file does not contain vulnerable `brace-expansion` versions. ([#279](https://github.com/trainstar/synchro/issues/279))

[Unreleased]: https://github.com/trainstar/synchro/compare/v0.4.0-rc.2...HEAD
[0.4.0-rc.2]: https://github.com/trainstar/synchro/compare/v0.4.0-rc.1...v0.4.0-rc.2
[0.4.0-rc.1]: https://github.com/trainstar/synchro/compare/v0.3.2...v0.4.0-rc.1
