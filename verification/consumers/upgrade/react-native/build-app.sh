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

case "$side" in
  predecessor) code=1 ;;
  candidate)
    code=2
    artifacts=${SYNCHRO_UPGRADE_ARTIFACT_DIR:?SYNCHRO_UPGRADE_ARTIFACT_DIR is required}
    ;;
  *) printf '%s\n' "unknown side: $side" >&2; exit 1 ;;
esac

if [ ! -d "$app" ]; then
  npx --yes "@react-native-community/cli@$cli_version" init SynchroUpgrade \
    --version 0.83.10 --directory "$app" --pm npm --skip-install
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
    cp android/app/build/outputs/apk/debug/app-debug.apk "$work/$side.apk"
    ;;
  ios)
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
abort "application Podfile platform was not found" unless content.sub!(/^platform :ios,.*$/, "platform :ios, '16.0'")
pods = "#{target}  #{synchro_pod}\n  pod 'GRDB.swift', :git => 'https://github.com/groue/GRDB.swift.git', :tag => 'v7.0.0'\n"
File.write(output, content.sub(target, pods))
RUBY
    (cd ios && pod install)
    if ! grep -F "Synchro ($version)" ios/Podfile.lock >/dev/null; then
      printf '%s\n' "React Native iOS $side did not install Synchro $version" >&2
      exit 1
    fi
    # A Debug build waits for a packager, so the application builds Release
    # and runs its embedded bundle.
    FORCE_BUNDLING=1 xcodebuild -quiet \
      -workspace ios/SynchroUpgrade.xcworkspace \
      -scheme SynchroUpgrade \
      -configuration Release \
      -sdk iphonesimulator \
      -derivedDataPath "$work/derived-data" \
      PRODUCT_BUNDLE_IDENTIFIER=dev.synchro.upgrade \
      IPHONEOS_DEPLOYMENT_TARGET=16.0 \
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
