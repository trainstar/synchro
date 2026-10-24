#!/bin/sh
# Builds the React Native upgrade application against one package side.
# Usage: build-app.sh <android|ios> <work directory> <predecessor|candidate> <version> <control URL>
# The predecessor resolves the published npm package and its published native
# SDK. The candidate resolves the local package artifacts in
# SYNCHRO_UPGRADE_ARTIFACT_DIR. Both sides build the same application project,
# so the second build is an application update, not a new application.
set -eu

platform=${1:?platform is required}
work=${2:?work directory is required}
side=${3:?side is required}
version=${4:?version is required}
control_url=${5:?control URL is required}
source_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)
app=$work/app
cli_version=20.2.0
host_preparation=$source_dir/../../react-native/prepare-android-host.py

case "$side" in
  predecessor) code=1 ;;
  candidate)
    code=2
    artifacts=${SYNCHRO_UPGRADE_ARTIFACT_DIR:?SYNCHRO_UPGRADE_ARTIFACT_DIR is required}
    ;;
  *) printf '%s\n' "unknown side: $side" >&2; exit 1 ;;
esac

if [ ! -d "$app" ]; then
  case "$platform" in
    android) host_rn_version=0.83.10 ;;
    ios) host_rn_version=0.83.10 ;;
    *) printf '%s\n' "unknown platform: $platform" >&2; exit 1 ;;
  esac
  npx --yes "@react-native-community/cli@$cli_version" init SynchroUpgrade \
    --version "$host_rn_version" --directory "$app" --pm npm --skip-install
  cp "$app/ios/Podfile" "$work/Podfile.template"
  (
    cd "$app"
    npm pkg set \
      "devDependencies.@react-native-community/cli=$cli_version" \
      "devDependencies.@react-native-community/cli-platform-android=$cli_version" \
      "devDependencies.@react-native-community/cli-platform-ios=$cli_version"
    # The packaged module and Kotlin SDK require core library desugaring.
    cat >> android/app/build.gradle <<'GRADLE'

android {
    compileOptions {
        coreLibraryDesugaringEnabled true
    }
}
dependencies {
    coreLibraryDesugaring("com.android.tools:desugar_jdk_libs:2.0.4")
}
GRADLE
  )
fi

if [ "$platform" = android ] && [ "$side" = candidate ]; then
  template=$work/current-rn-template
  original_application_id=$(sed -n 's/^[[:space:]]*applicationId[[:space:]]*"\([^"]*\)".*/\1/p' "$app/android/app/build.gradle")
  test -n "$original_application_id" || { printf '%s\n' "React Native predecessor application id is missing" >&2; exit 1; }
  original_keystore=$app/android/app/debug.keystore
  test -f "$original_keystore" || { printf '%s\n' "React Native predecessor debug keystore is missing" >&2; exit 1; }
  cp "$original_keystore" "$work/predecessor-debug.keystore"
  rm -rf "$template"
  npx --yes "@react-native-community/cli@$cli_version" init SynchroUpgrade \
    --version 0.88.0 --directory "$template" --pm npm --skip-install
  current_application_id=$(sed -n 's/^[[:space:]]*applicationId[[:space:]]*"\([^"]*\)".*/\1/p' "$template/android/app/build.gradle")
  if [ "$current_application_id" != "$original_application_id" ]; then
    printf '%s\n' "React Native current template changed the application id" >&2
    exit 1
  fi
  for config in package.json babel.config.js metro.config.js tsconfig.json; do
    test -f "$template/$config" || { printf '%s\n' "React Native current template is missing $config" >&2; exit 1; }
    cp "$template/$config" "$app/$config"
  done
  rm -f "$app/package-lock.json"
  rm -rf "$app/android"
  cp -R "$template/android" "$app/android"
  cp "$work/predecessor-debug.keystore" "$app/android/app/debug.keystore"
  cmp -s "$work/predecessor-debug.keystore" "$app/android/app/debug.keystore" || {
    printf '%s\n' "React Native current template did not preserve the debug keystore" >&2
    exit 1
  }
  (
    cd "$app"
    npm pkg set \
      "devDependencies.@react-native-community/cli=$cli_version" \
      "devDependencies.@react-native-community/cli-platform-android=$cli_version" \
      "devDependencies.@react-native-community/cli-platform-ios=$cli_version"
    python3 "$host_preparation" "$app/android" 0.88.0
    python3 - "$app/android/app/src/main/AndroidManifest.xml" <<'PY'
import re
import sys
from pathlib import Path

manifest = Path(sys.argv[1])
content = manifest.read_text()
permission = '<uses-permission android:name="android.permission.ACCESS_LOCAL_NETWORK" />'
if permission in content:
    raise SystemExit("generated Android manifest already declares ACCESS_LOCAL_NETWORK")
updated, count = re.subn(r"(<manifest\b[^>]*>)", r"\1\n    " + permission, content, count=1)
if count != 1:
    raise SystemExit("generated Android manifest has an unexpected root element")
manifest.write_text(updated)
PY
    # The packaged module and Kotlin SDK require core library desugaring.
    cat >> android/app/build.gradle <<'GRADLE'

android {
    compileOptions {
        coreLibraryDesugaringEnabled true
    }
}
dependencies {
    coreLibraryDesugaring("com.android.tools:desugar_jdk_libs:2.0.4")
}
GRADLE
  )
  app=$(CDPATH= cd -- "$app" && pwd -P)
fi
cd "$app"
cp "$source_dir/App.tsx" App.tsx
if [ "$side" = predecessor ]; then
  npm install --ignore-scripts --save-exact "@trainstar/synchro-react-native@$version"
else
  npm install --ignore-scripts --save-exact "$artifacts/npm/trainstar-synchro-react-native-$version.tgz"
fi
installed=$(node -p "require('@trainstar/synchro-react-native/package.json').version")
if [ "$installed" != "$version" ]; then
  printf '%s\n' "installed React Native package is $installed, want $version" >&2
  exit 1
fi
if [ "$platform" = android ] && [ "$side" = candidate ]; then
  package_root=$(node -p "require('path').dirname(require.resolve('@trainstar/synchro-react-native/package.json'))")
  package_root=$(cd "$package_root" && pwd -P)
  expected_package_root=$app/node_modules/@trainstar/synchro-react-native
  if [ "$package_root" != "$expected_package_root" ]; then
    printf '%s\n' "React Native package resolved outside the isolated upgrade application" >&2
    exit 1
  fi
  tooling_helper=$package_root/tooling-patches/apply.cjs
  test -f "$tooling_helper" || { printf '%s\n' "React Native tooling helper is missing" >&2; exit 1; }
  node "$tooling_helper" cli
  node "$tooling_helper" rngp
fi
resolved=$(node -p "require('./package-lock.json').packages['node_modules/@trainstar/synchro-react-native'].resolved")
printf '%s\n' "React Native $side installed @trainstar/synchro-react-native $installed from $resolved"
printf "export const controlURL = '%s';\nexport const packageVersion = '%s';\n" "$control_url" "$installed" > upgradeControl.ts
npx tsc --noEmit

case "$platform" in
  android)
    sed -i.bak "s/versionCode [0-9][0-9]*/versionCode $code/" android/app/build.gradle
    rm -f android/app/build.gradle.bak
    mkdir -p android/app/src/main/assets android/app/src/main/res
    npx react-native bundle --platform android --dev false --entry-file index.js \
      --bundle-output android/app/src/main/assets/index.android.bundle \
      --assets-dest android/app/src/main/res
    set --
    if [ "$side" = candidate ]; then
      cat > "$work/candidate-repository.gradle" <<GRADLE
allprojects {
    repositories {
        exclusiveContent {
            forRepository { maven { url = uri("$artifacts/maven") } }
            filter { includeGroup("fit.trainstar") }
        }
    }
}
GRADLE
      set -- --init-script "$work/candidate-repository.gradle"
    fi
    (
      cd android
      ANDROID_HOME="${ANDROID_HOME:?ANDROID_HOME is required}" ANDROID_SDK_ROOT="$ANDROID_HOME" \
      JAVA_HOME="${ANDROID_JAVA_HOME:?ANDROID_JAVA_HOME is required}" PATH="$ANDROID_JAVA_HOME/bin:$PATH" \
        ./gradlew --no-daemon "$@" -PsynchroVersion="$version" \
          :app:assembleDebug :app:dependencyInsight --dependency fit.trainstar:synchro \
          --configuration debugRuntimeClasspath > "$work/$side-gradle.log"
    )
    if ! grep -F "fit.trainstar:synchro:$version" "$work/$side-gradle.log" >/dev/null; then
      cat "$work/$side-gradle.log" >&2
      printf '%s\n' "React Native Android $side did not resolve Synchro $version" >&2
      exit 1
    fi
    printf '%s\n' "React Native Android $side resolved fit.trainstar:synchro:$version"
    cp android/app/build/outputs/apk/debug/app-debug.apk "$work/$side.apk"
    ;;
  ios)
    sed 's/withModuleName: "SynchroConsumer"/withModuleName: "SynchroUpgrade"/' \
      "$source_dir/../../react-native/AppDelegate.swift" > ios/SynchroUpgrade/AppDelegate.swift
    plist=ios/SynchroUpgrade/Info.plist
    python3 - "$source_dir/../../../../clients/react-native/example/ios/SynchroReactNativeExample/Info.plist" "$plist" <<'PY'
import plistlib
import sys
with open(sys.argv[1], 'rb') as source, open(sys.argv[2], 'rb') as target:
    manifest = plistlib.load(source)['UIApplicationSceneManifest']
    app = plistlib.load(target)
app['UIApplicationSceneManifest'] = manifest
with open(sys.argv[2], 'wb') as target:
    plistlib.dump(app, target, sort_keys=False)
PY
    if [ "$side" = predecessor ]; then
      synchro_pod="pod 'Synchro', :git => 'https://github.com/trainstar/synchro.git', :tag => 'v$version'"
    else
      synchro_pod="pod 'Synchro', :path => '$artifacts/apple/Synchro'"
    fi
    ruby - "$work/Podfile.template" ios/Podfile "$synchro_pod" <<'RUBY'
template, output, synchro_pod = ARGV
content = File.read(template)
target = "target 'SynchroUpgrade' do\n"
abort "application Podfile target was not found" unless content.include?(target)
abort "application Podfile platform was not found" unless content.sub!(/^platform :ios,.*$/, "platform :ios, '17.0'")
pods = "#{target}  #{synchro_pod}\n  pod 'GRDB.swift', :git => 'https://github.com/groue/GRDB.swift.git', :tag => 'v7.8.0'\n"
File.write(output, content.sub(target, pods))
RUBY
    (cd ios && pod install)
    if ! grep -F "Synchro ($version)" ios/Podfile.lock >/dev/null; then
      printf '%s\n' "React Native iOS $side did not install Synchro $version" >&2
      exit 1
    fi
    printf '%s\n' "React Native iOS $side installed:"
    sed -n '/^CHECKOUT OPTIONS:/,/^$/p; /^  - Synchro (/p' ios/Podfile.lock
    # A Debug build waits for a packager, so the application builds Release
    # and runs its embedded bundle.
    FORCE_BUNDLING=1 xcodebuild -quiet \
      -workspace ios/SynchroUpgrade.xcworkspace \
      -scheme SynchroUpgrade \
      -configuration Release \
      -sdk iphonesimulator \
      -derivedDataPath "$work/derived-data" \
      PRODUCT_BUNDLE_IDENTIFIER=dev.synchro.upgrade \
      IPHONEOS_DEPLOYMENT_TARGET=17.0 \
      CURRENT_PROJECT_VERSION="$code" \
      CODE_SIGNING_ALLOWED=NO \
      DEBUG_INFORMATION_FORMAT=dwarf \
      build
    rm -rf "$work/$side.app"
    cp -R "$work/derived-data/Build/Products/Release-iphonesimulator/SynchroUpgrade.app" "$work/$side.app"
    test -f "$work/$side.app/main.jsbundle"
    ;;
  *) printf '%s\n' "unknown platform: $platform" >&2; exit 1 ;;
esac
