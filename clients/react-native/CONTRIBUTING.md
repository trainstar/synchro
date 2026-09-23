# Contributing

Read the [code of conduct](CODE_OF_CONDUCT.md) before contributing.

## Development Workflow

This project is a monorepo managed using [Yarn workspaces](https://yarnpkg.com/features/workspaces). It contains the following packages:

- The library package in `clients/react-native/`, which is the workspace root and not the repository root.
- An example app in the `example/` directory.

Use Node `22.20.0` for contributor tooling, as pinned in [`.nvmrc`](.nvmrc).
The published package supports Node `>=20.19.4` at runtime.
The runtime floor does not replace the contributor toolchain pin.

From `clients/react-native`, run `yarn` to install the required workspace dependencies:

```sh
yarn
```

> Since the project relies on Yarn workspaces, you cannot use [`npm`](https://github.com/npm/cli) for development without manually migrating.

The [example app](example/README.md) demonstrates the library integration.
It is the required Detox harness.
Interactive Metro runs support development and do not provide validation evidence.

The example uses the local library.
Metro can load JavaScript changes without a native rebuild.
Rebuild the example after changing native code.

After pod installation, open `example/ios/SynchroReactNativeExample.xcworkspace` in Xcode.
The bridge pod is named `SynchroReactNative`; its source files are in `ios/`.

Open `example/android` in Android Studio for the Android application.
The bridge source files are in `android/src/main/`.

For development only, run these commands from `clients/react-native`:

To start Metro:

```sh
yarn example start
```

To run the example app on Android:

```sh
yarn example android
```

To run the example app on iOS:

```sh
yarn example ios
```

To confirm the new architecture during development, inspect the Metro logs for a message like this:

```sh
Running "SynchroReactNativeExample" with {"fabric":true,"initialProps":{"concurrentRoot":true},"rootTag":1}
```

Note the `"fabric":true` and `"concurrentRoot":true` properties.

Run validation through Make from the repository root:

```sh
make lint-rn
make test-rn-unit
make test-rn-e2e-ios
make test-rn-e2e-android
```

Use `make test-rn` when both end-to-end platforms are available.
Do not use direct Jest or Detox commands as a substitute for these structured gates.

The end-to-end targets prepare repository-owned test fixtures and seeds.
Use the [development guide](../../docs/src/content/docs/getting-started/development.mdx) for the fixture prerequisites and Make sequence.

## Sending A Pull Request

Use `dev` as the base for ordinary development pull requests.
The default branch, `master`, contains stable release code.
Promote a verified release from `dev` through a pull request into `master`.
Create urgent stable hotfixes from `master`, and merge their corrections back into `dev`.
Delete temporary branches only after their current work is merged.

> **Working on your first pull request?** You can learn how from this _free_ series: [How to Contribute to an Open Source Project on GitHub](https://app.egghead.io/playlists/how-to-contribute-to-an-open-source-project-on-github).

When you're sending a pull request:

- Prefer small pull requests focused on one change.
- Verify that linters and tests are passing.
- Review the documentation to make sure it looks good.
- Describe the change and applicable Make results in the pull request.
- For pull requests that change the API or implementation, discuss with maintainers first by opening an issue.
