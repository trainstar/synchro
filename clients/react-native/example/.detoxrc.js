// The Android build resolves the Synchro Kotlin SDK only from the Maven repository that
// make release-kotlin-local publishes to. The Makefile exports SYNCHRO_MAVEN_REPO. The shell
// stops the build when it is not set, so the build cannot use the default Maven local repository.
const gradleMavenRepo =
  '"-Dmaven.repo.local=${SYNCHRO_MAVEN_REPO:?Set SYNCHRO_MAVEN_REPO to the repository of make release-kotlin-local}"';

/** @type {import('detox').DetoxConfig} */
module.exports = {
  testRunner: {
    args: {
      $0: 'jest',
      _: ['e2e/conformance.test.ts', 'e2e/sync.test.ts'],
      config: 'e2e/jest.config.js',
    },
    jest: {
      setupTimeout: 120000,
    },
  },
  apps: {
    'ios.debug': {
      type: 'ios.app',
      binaryPath:
        'ios/build/Build/Products/Debug-iphonesimulator/SynchroReactNativeExample.app',
      build:
        "FORCE_BUNDLING=1 RCT_NO_LAUNCH_PACKAGER=1 xcodebuild -quiet -workspace ios/SynchroReactNativeExample.xcworkspace -scheme SynchroReactNativeExample -configuration Debug -sdk iphonesimulator -destination 'generic/platform=iOS Simulator' -derivedDataPath ios/build ONLY_ACTIVE_ARCH=YES SWIFT_ACTIVE_COMPILATION_CONDITIONS='$(inherited) DETOX_E2E'",
    },
    'android.debug': {
      type: 'android.apk',
      binaryPath: 'android/app/build/outputs/apk/debug/app-debug.apk',
      testBinaryPath:
        'android/app/build/outputs/apk/androidTest/debug/app-debug-androidTest.apk',
      build:
        `cd android && ./gradlew ${gradleMavenRepo} assembleDebug assembleAndroidTest -DtestBuildType=debug -PdetoxBundleDebug=true`,
      reversePorts: [8081],
    },
    'android.release': {
      type: 'android.apk',
      binaryPath: 'android/app/build/outputs/apk/release/app-release.apk',
      testBinaryPath:
        'android/app/build/outputs/apk/androidTest/debug/app-debug-androidTest.apk',
      build:
        `cd android && ./gradlew ${gradleMavenRepo} assembleRelease assembleDebugAndroidTest`,
      reversePorts: [8081],
    },
  },
  devices: {
    simulator: {
      type: 'ios.simulator',
      device: process.env.IOS_SIMULATOR_UDID
        ? { id: process.env.IOS_SIMULATOR_UDID }
        : { type: 'iPhone SE (3rd generation)' },
    },
    // A run uses only the booted device that ANDROID_SERIAL names. Two gates on
    // one host can then never share, boot, or stop each other's emulator.
    emulator: {
      type: 'android.attached',
      device: {
        adbName: process.env.ANDROID_SERIAL
          ? `^${process.env.ANDROID_SERIAL.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}$`
          : undefined,
      },
    },
  },
  configurations: {
    'ios.sim.debug': {
      device: 'simulator',
      app: 'ios.debug',
    },
    'android.emu.debug': {
      device: 'emulator',
      app: 'android.debug',
    },
    'android.emu.release': {
      device: 'emulator',
      app: 'android.release',
    },
  },
};
