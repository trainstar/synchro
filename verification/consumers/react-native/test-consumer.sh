#!/bin/sh
set -eu

platform=${1:?platform is required}
artifact_dir=${2:?artifact directory is required}
version=${3:?version is required}

case "$platform" in
  ios|android) ;;
  *) printf '%s\n' "unsupported React Native consumer platform: $platform" >&2; exit 1 ;;
esac

tarball="$artifact_dir/npm/trainstar-synchro-react-native-$version.tgz"
apple_dir="$artifact_dir/apple/Synchro"
maven_dir="$artifact_dir/maven"
test -f "$tarball"
case "$platform" in
  ios) test -f "$apple_dir/Synchro.podspec" ;;
  android) test -d "$maven_dir/fit/trainstar/synchro/$version" ;;
esac

work_dir=$(mktemp -d "${TMPDIR:-/tmp}/synchro-rn-consumer.XXXXXX")
work_dir=$(cd "$work_dir" && pwd -P)
trap 'rm -rf "$work_dir"' EXIT HUP INT TERM

npx --yes @react-native-community/cli@20.0.0 init SynchroConsumer \
  --version 0.83.0 \
  --directory "$work_dir/app" \
  --pm npm \
  --skip-install

cp "$(dirname "$0")/App.tsx" "$work_dir/app/App.tsx"
(
  cd "$work_dir/app"
  npm install --ignore-scripts --save-exact "$tarball"
  package_root=$(node -p "require('path').dirname(require.resolve('@trainstar/synchro-react-native/package.json'))")
  package_root=$(cd "$package_root" && pwd -P)
  case "$package_root" in
    "$work_dir"/*/node_modules/@trainstar/synchro-react-native) ;;
    *) printf '%s\n' "React Native package resolved outside the isolated consumer: $package_root" >&2; exit 1 ;;
  esac
  test "$(node -p "require('@trainstar/synchro-react-native/package.json').version")" = "$version"
  npx tsc --noEmit
)

case "$platform" in
  ios)
    ruby - "$work_dir/app/ios/Podfile" "$apple_dir" <<'RUBY'
podfile, apple_dir = ARGV
content = File.read(podfile)
target = "target 'SynchroConsumer' do\n"
abort "consumer Podfile target was not found" unless content.include?(target)
abort "consumer Podfile platform was not found" unless content.sub!(/^platform :ios,.*$/, "platform :ios, '16.0'")
pods = <<~PODS
  target 'SynchroConsumer' do
    pod 'Synchro', :path => '#{apple_dir}'
    pod 'GRDB.swift', :git => 'https://github.com/groue/GRDB.swift.git', :tag => 'v7.0.0'
PODS
File.write(podfile, content.sub(target, pods))
RUBY
    (
      cd "$work_dir/app/ios"
      pod install
      FORCE_BUNDLING=1 xcodebuild \
        -workspace SynchroConsumer.xcworkspace \
        -scheme SynchroConsumer \
        -configuration Debug \
        -sdk iphonesimulator \
        -derivedDataPath "$work_dir/derived-data" \
        PRODUCT_BUNDLE_IDENTIFIER=dev.synchro.consumer \
        IPHONEOS_DEPLOYMENT_TARGET=16.0 \
        CODE_SIGNING_ALLOWED=NO \
        build
    )
    simulator_udid=${IOS_SIMULATOR_UDID:-$(xcrun simctl list devices booted -j | ruby -rjson -e 'devices = JSON.parse(STDIN.read).fetch("devices").values.flatten; device = devices.find { |item| item["state"] == "Booted" }; abort "no booted iOS simulator" unless device; puts device.fetch("udid")')}
    if [ -n "${SUPPORT_PLATFORM_VERSION:-}" ]; then
      simulator_version=$(xcrun simctl list devices -j | ruby -rjson -e 'udid = ARGV.fetch(0); JSON.parse(STDIN.read).fetch("devices").each { |runtime, devices| if devices.any? { |device| device["udid"] == udid }; puts runtime.sub(/^.*\.iOS-/, "").tr("-", "."); exit; end }; abort "simulator runtime was not found"' "$simulator_udid")
      case "$SUPPORT_PLATFORM_VERSION" in
        *.*) test "$simulator_version" = "$SUPPORT_PLATFORM_VERSION" ;;
        *) test "${simulator_version%%.*}" = "$SUPPORT_PLATFORM_VERSION" ;;
      esac
    fi
    app_path="$work_dir/derived-data/Build/Products/Debug-iphonesimulator/SynchroConsumer.app"
    test -f "$app_path/main.jsbundle"
    xcrun simctl install "$simulator_udid" "$app_path"
    xcrun simctl launch "$simulator_udid" dev.synchro.consumer >/dev/null
    passed=0
    for _ in $(seq 1 60); do
      container=$(xcrun simctl get_app_container "$simulator_udid" dev.synchro.consumer data 2>/dev/null || true)
      if [ -n "$container" ] && [ -f "$container/Documents/consumer.db" ]; then
        value=$(sqlite3 "$container/Documents/consumer.db" "SELECT value FROM consumer_probe WHERE id = 'probe';" 2>/dev/null || true)
        if [ "$value" = "packaged" ]; then passed=1; break; fi
      fi
      sleep 1
    done
    xcrun simctl terminate "$simulator_udid" dev.synchro.consumer >/dev/null 2>&1 || true
    if [ "$passed" -ne 1 ]; then
      printf '%s\n' "Packaged React Native iOS consumer did not write its SQLite canary" >&2
      exit 1
    fi
    ;;
  android)
    cat > "$work_dir/synchro-repository.gradle" <<EOF
allprojects {
    repositories {
        exclusiveContent {
            forRepository { maven { url = uri("$maven_dir") } }
            filter { includeGroup("fit.trainstar") }
        }
    }
}
EOF
    (
      cd "$work_dir/app"
      mkdir -p android/app/src/main/assets android/app/src/main/res
      npx react-native bundle \
        --platform android \
        --dev false \
        --entry-file index.js \
        --bundle-output android/app/src/main/assets/index.android.bundle \
        --assets-dest android/app/src/main/res
    )
    (
      cd "$work_dir/app/android"
      ANDROID_HOME="${ANDROID_HOME:?ANDROID_HOME is required}" \
      ANDROID_SDK_ROOT="${ANDROID_HOME}" \
      JAVA_HOME="${ANDROID_JAVA_HOME:?ANDROID_JAVA_HOME is required}" \
      PATH="$ANDROID_JAVA_HOME/bin:$PATH" \
      ./gradlew \
        --no-daemon \
        --init-script "$work_dir/synchro-repository.gradle" \
         -PsynchroVersion="$version" \
         :app:assembleDebug
    )
    adb="${ANDROID_HOME:?ANDROID_HOME is required}/platform-tools/adb"
    test -x "$adb"
    "$adb" get-state >/dev/null
    "$adb" install -r "$work_dir/app/android/app/build/outputs/apk/debug/app-debug.apk" >/dev/null
    "$adb" shell am force-stop com.synchroconsumer
    "$adb" shell am start -W -n com.synchroconsumer/.MainActivity >/dev/null
    database="$work_dir/consumer.db"
    passed=0
    for _ in $(seq 1 60); do
      if "$adb" exec-out run-as com.synchroconsumer cat databases/consumer.db > "$database" 2>/dev/null; then
        value=$(sqlite3 "$database" "SELECT value FROM consumer_probe WHERE id = 'probe';" 2>/dev/null || true)
        if [ "$value" = "packaged" ]; then passed=1; break; fi
      fi
      sleep 1
    done
    "$adb" shell am force-stop com.synchroconsumer
    "$adb" uninstall com.synchroconsumer >/dev/null
    if [ "$passed" -ne 1 ]; then
      printf '%s\n' "Packaged React Native Android consumer did not write its SQLite canary" >&2
      exit 1
    fi
    ;;
esac

printf '%s\n' "Packaged React Native $platform consumer passed"
