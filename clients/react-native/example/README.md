# Synchro React Native Example

This app is the RN bridge verification harness for `@trainstar/synchro-react-native`. It is not product sample UI. Each button exercises a specific SDK behavior and updates a visible `PASS` / `FAIL` badge for Detox.

## Requirements

- Node `20+`
- Xcode with an iOS simulator
- Android SDK and emulator for Android verification
- JDK `17` for Android builds
- Running Synchro test server via the repo `make` targets

## Common Commands

From `clients/react-native/example`:

```sh
npm run ios
npm run build:ios
npm run build:android
npm run detox:build:ios
npm run detox:test:ios
npm run detox:build:android
npm run detox:test:android
```

From the repo root:

```sh
make test-rn-unit
make test-rn-e2e-ios
make test-rn-e2e-android
```

The Make targets start and configure the `synchrod` test server automatically.

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

- The harness assumes the repo test JWT secret and test server settings used by the existing Swift and Kotlin integration flows.
- iOS Detox runs build a bundled JS app instead of relying on Metro. The local Synchro adapter listens on `8091`, which stays off Metro's default `8081` port.
- Android verification is required for shipability. iOS-only green runs are not sufficient.
- `seed.db` is a canonical bundled offline seed asset for the harness. iOS packages it directly from the example root, and Android copies the same file into app assets during `preBuild`.
