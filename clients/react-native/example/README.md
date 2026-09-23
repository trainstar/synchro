# Synchro React Native Verification Harness

This app is the React Native bridge verification harness for `@trainstar/synchro-react-native`.
It is not a product tutorial or sample UI.
Use the [primary Quickstart](../../../docs/src/content/docs/getting-started/quickstart.mdx) for application setup.

## Requirements

- Node `22.20.0` for repository contributor tooling

The published package runtime supports Node `>=20.19.4`.
That runtime floor does not replace the contributor toolchain pin.
Use the [development guide](../../../docs/src/content/docs/getting-started/development.mdx) for fixture prerequisites and the Make sequence.

## Development Commands

From `clients/react-native`, use Metro commands only for development:

```sh
yarn example start
yarn example ios
yarn example android
```

## Validation Commands

From the repository root:

```sh
make lint-rn
make test-rn-unit
make test-rn-e2e-ios
make test-rn-e2e-android
```

Use `make test-rn` when both end-to-end platforms are available.
Do not replace these targets with direct Jest or Detox commands.

The end-to-end targets restart the repository-owned extension-backed test adapter and create a fresh test seed.
The development guide defines their platform and fixture prerequisites.

## What The Harness Covers

- initialization and auth callback wiring
- query / execute
- read and write transactions
- rollback, timeout, and recovery after timeout
- start / stop / syncNow lifecycle
- push / pull round trip
- conflict delivery
- multi-user isolation
- native error mapping

## Notes

- The harness uses the repository test JWT secret and test server settings.
- iOS Detox runs build a bundled JS app instead of relying on Metro. The local Synchro adapter listens on `8091`, which stays off Metro's default `8081` port.
- Android verification is required for shipability. iOS-only green runs are not sufficient.
- `seed.db` is the pinned offline seed for the harness. `seed.db.sha256` records its digest.
- Run `make refresh-rn-seed` from the repository root after an intentional seed change.
- `make rn-seed-asset` stages the pinned seed under the ignored `verification/` directory for native builds.
- Detox stages a fresh ignored seed from its ephemeral test server because portable seed receipts bind to one server deployment.
