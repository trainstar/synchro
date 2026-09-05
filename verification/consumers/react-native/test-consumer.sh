#!/bin/sh
set -eu

platform=${1:?platform is required}
artifact_dir=${2:?artifact directory is required}
version=${3:?version is required}
mode=${4:-smoke}

case "$mode" in
  smoke)
    cell_id=${PACKAGED_SMOKE_CELL_ID:?PACKAGED_SMOKE_CELL_ID is required}
    cell_result=${PACKAGED_SMOKE_CELL_RESULT:?PACKAGED_SMOKE_CELL_RESULT is required}
    ;;
  build-only) ;;
  *) printf '%s\n' "unsupported React Native consumer mode: $mode" >&2; exit 1 ;;
esac

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

source_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)
repo_root=$(CDPATH= cd -- "$source_dir/../../.." && pwd -P)
tool="$repo_root/verification/packaged_smoke.py"
tmp_root=${PACKAGED_SMOKE_TMP_ROOT:-$repo_root/.ignore/r2/tmp}
mkdir -p "$tmp_root"
work_dir=$(mktemp -d "$tmp_root/synchro-rn-consumer.XXXXXX")
work_dir=$(cd "$work_dir" && pwd -P)
installed_platform=
simulator_udid=
adb=
reverse_port=
cleanup() {
  case "$installed_platform" in
    ios) xcrun simctl uninstall "$simulator_udid" dev.synchro.consumer >/dev/null 2>&1 || true ;;
    android)
      "$adb" shell am force-stop com.synchroconsumer >/dev/null 2>&1 || true
      "$adb" uninstall com.synchroconsumer >/dev/null 2>&1 || true
      ;;
  esac
  if [ -n "$adb" ] && [ -n "$reverse_port" ]; then
    "$adb" reverse --remove "tcp:$reverse_port" >/dev/null 2>&1 || true
  fi
  rm -rf "$work_dir"
}
trap cleanup EXIT HUP INT TERM

npx --yes @react-native-community/cli@20.0.0 init SynchroConsumer \
  --version 0.83.0 \
  --directory "$work_dir/app" \
  --pm npm \
  --skip-install

cp "$(dirname "$0")/App.tsx" "$work_dir/app/App.tsx"
cp "$(dirname "$0")/packagedSmokeConfig.ts" "$work_dir/app/packagedSmokeConfig.ts"
if [ "$mode" = "smoke" ]; then
  python3 "$tool" config \
    --cell "$cell_id" \
    --platform "react-native-$platform" \
    --output "$work_dir/config.json"
  python3 "$tool" config-to-typescript \
    --config "$work_dir/config.json" \
    --output "$work_dir/app/packagedSmokeConfig.ts"
fi
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
    if [ "$mode" = "build-only" ]; then
      printf '%s\n' "Packaged React Native iOS consumer build passed"
      exit 0
    fi
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
    xcrun simctl uninstall "$simulator_udid" dev.synchro.consumer >/dev/null 2>&1 || true
    xcrun simctl install "$simulator_udid" "$app_path"
    installed_platform=ios
    launch_output=$(xcrun simctl launch "$simulator_udid" dev.synchro.consumer)
    initial_pid=${launch_output##*: }
    case "$initial_pid" in *[!0-9]*|'') printf '%s\n' "React Native iOS initial process id is invalid" >&2; exit 1 ;; esac
    container=$(xcrun simctl get_app_container "$simulator_udid" dev.synchro.consumer data)
    pending=0
    ready=0
    for _ in $(seq 1 120); do
      if [ -f "$container/Documents/consumer.db" ]; then
        pending=$(sqlite3 "$container/Documents/consumer.db" "SELECT COUNT(*) FROM _synchro_pending_changes WHERE lifecycle_state NOT IN ('accepted','rejected','superseded_before_send','cancelled_before_send');" 2>/dev/null || true)
        durable=$(sqlite3 "$container/Documents/consumer.db" "SELECT ship_address FROM orders WHERE id = '$(python3 "$tool" config-value --config "$work_dir/config.json" --field order_id)';" 2>/dev/null || true)
        if [ "$pending" = "1" ] && [ "$durable" = '{"street":"Packaged Durable"}' ]; then
          ready=1
          break
        fi
      fi
      if ! kill -0 "$initial_pid" >/dev/null 2>&1; then break; fi
      sleep 1
    done
    if [ "$ready" -ne 1 ]; then
      # The app names its failure in the simulator log, and the phase leaves
      # no other trace, so the recent app log must be reported or it is lost.
      xcrun simctl spawn "$simulator_udid" log show --last 3m --style compact \
        --predicate 'processImagePath CONTAINS "SynchroConsumer"' 2>/dev/null \
        | grep -vE "com.apple" | tail -40 >&2 || true
      printf '%s\n' "Packaged React Native iOS initial phase did not become ready" >&2
      exit 1
    fi
    python3 "$tool" phase-result --output "$work_dir/initial.json" --phase initial --pid "$initial_pid" --pending-count "$pending"
    kill -9 "$initial_pid"
    killed=0
    for _ in $(seq 1 30); do
      if ! kill -0 "$initial_pid" >/dev/null 2>&1; then killed=1; break; fi
      sleep 1
    done
    if [ "$killed" -ne 1 ]; then
      printf '%s\n' "Packaged React Native iOS process kill was not observed" >&2
      exit 1
    fi
    launch_output=$(xcrun simctl launch "$simulator_udid" dev.synchro.consumer)
    resume_pid=${launch_output##*: }
    case "$resume_pid" in *[!0-9]*|'') printf '%s\n' "React Native iOS resume process id is invalid" >&2; exit 1 ;; esac
    if [ "$resume_pid" = "$initial_pid" ]; then
      printf '%s\n' "Packaged React Native iOS resume reused the killed process" >&2
      exit 1
    fi
    resumed=0
    for _ in $(seq 1 120); do
      pending=$(sqlite3 "$container/Documents/consumer.db" "SELECT COUNT(*) FROM _synchro_pending_changes WHERE lifecycle_state NOT IN ('accepted','rejected','superseded_before_send','cancelled_before_send');" 2>/dev/null || true)
      if [ "$pending" = "0" ]; then resumed=1; break; fi
      if ! kill -0 "$resume_pid" >/dev/null 2>&1; then break; fi
      sleep 1
    done
    xcrun simctl terminate "$simulator_udid" dev.synchro.consumer >/dev/null 2>&1 || true
    if [ "$resumed" -ne 1 ]; then
      printf '%s\n' "Packaged React Native iOS resume phase did not pass" >&2
      exit 1
    fi
    python3 "$tool" phase-result --output "$work_dir/resume.json" --phase resume --pid "$resume_pid" --pending-count 0
    native_artifact="$artifact_dir/apple/synchro-spm-$version.tar.gz"
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
    if [ "$mode" = "build-only" ]; then
      printf '%s\n' "Packaged React Native Android consumer build passed"
      exit 0
    fi
    adb="${ANDROID_HOME:?ANDROID_HOME is required}/platform-tools/adb"
    test -x "$adb"
    "$adb" get-state >/dev/null
    server_url=$(python3 "$tool" config-value --config "$work_dir/config.json" --field server_url)
    reverse_port=$(python3 -c 'import sys, urllib.parse; value=urllib.parse.urlsplit(sys.argv[1]); print(value.port or (443 if value.scheme == "https" else 80)) if value.hostname in {"127.0.0.1", "localhost"} else None' "$server_url")
    if [ -n "$reverse_port" ]; then
      "$adb" reverse "tcp:$reverse_port" "tcp:$reverse_port"
    fi
    "$adb" uninstall com.synchroconsumer >/dev/null 2>&1 || true
    "$adb" install "$work_dir/app/android/app/build/outputs/apk/debug/app-debug.apk" >/dev/null
    installed_platform=android
    "$adb" shell pm clear com.synchroconsumer >/dev/null
    "$adb" shell am force-stop com.synchroconsumer
    "$adb" shell am start -W -n com.synchroconsumer/.MainActivity >/dev/null
    initial_pid=$("$adb" shell pidof com.synchroconsumer | tr -d '\r')
    case "$initial_pid" in *[!0-9]*|'') printf '%s\n' "React Native Android initial process id is invalid" >&2; exit 1 ;; esac
    database="$work_dir/consumer.db"
    pending=0
    ready=0
    order_id=$(python3 "$tool" config-value --config "$work_dir/config.json" --field order_id)
    copy_android_database() {
      if "$adb" exec-out run-as com.synchroconsumer cat databases/consumer.db > "$database" 2>/dev/null; then
        "$adb" exec-out run-as com.synchroconsumer cat databases/consumer.db-wal > "$database-wal" 2>/dev/null || rm -f "$database-wal"
        "$adb" exec-out run-as com.synchroconsumer cat databases/consumer.db-shm > "$database-shm" 2>/dev/null || rm -f "$database-shm"
        return 0
      fi
      return 1
    }
    for _ in $(seq 1 120); do
      if copy_android_database; then
        pending=$(sqlite3 "$database" "SELECT COUNT(*) FROM _synchro_pending_changes WHERE lifecycle_state NOT IN ('accepted','rejected','superseded_before_send','cancelled_before_send');" 2>/dev/null || true)
        durable=$(sqlite3 "$database" "SELECT ship_address FROM orders WHERE id = '$order_id';" 2>/dev/null || true)
        if [ "$pending" = "1" ] && [ "$durable" = '{"street":"Packaged Durable"}' ]; then ready=1; break; fi
      fi
      current_pid=$("$adb" shell pidof com.synchroconsumer 2>/dev/null | tr -d '\r' || true)
      if [ "$current_pid" != "$initial_pid" ]; then break; fi
      sleep 1
    done
    if [ "$ready" -ne 1 ]; then
      printf '%s\n' "Packaged React Native Android initial phase did not become ready" >&2
      exit 1
    fi
    python3 "$tool" phase-result --output "$work_dir/initial.json" --phase initial --pid "$initial_pid" --pending-count "$pending"
    set +e
    "$adb" shell run-as com.synchroconsumer kill -9 "$initial_pid"
    kill_status=$?
    set -e
    case "$kill_status" in 0|137) ;; *) printf '%s\n' "React Native Android kill command failed" >&2; exit 1 ;; esac
    killed=0
    for _ in $(seq 1 30); do
      current_pid=$("$adb" shell pidof com.synchroconsumer 2>/dev/null | tr -d '\r' || true)
      if [ "$current_pid" != "$initial_pid" ]; then killed=1; break; fi
      sleep 1
    done
    if [ "$killed" -ne 1 ]; then
      printf '%s\n' "Packaged React Native Android process kill was not observed" >&2
      exit 1
    fi
    "$adb" shell am start -W -n com.synchroconsumer/.MainActivity >/dev/null
    resume_pid=$("$adb" shell pidof com.synchroconsumer | tr -d '\r')
    case "$resume_pid" in *[!0-9]*|'') printf '%s\n' "React Native Android resume process id is invalid" >&2; exit 1 ;; esac
    if [ "$resume_pid" = "$initial_pid" ]; then
      printf '%s\n' "Packaged React Native Android resume reused the killed process" >&2
      exit 1
    fi
    resumed=0
    for _ in $(seq 1 120); do
      if copy_android_database; then
        pending=$(sqlite3 "$database" "SELECT COUNT(*) FROM _synchro_pending_changes WHERE lifecycle_state NOT IN ('accepted','rejected','superseded_before_send','cancelled_before_send');" 2>/dev/null || true)
        if [ "$pending" = "0" ]; then resumed=1; break; fi
      fi
      current_pid=$("$adb" shell pidof com.synchroconsumer 2>/dev/null | tr -d '\r' || true)
      if [ "$current_pid" != "$resume_pid" ]; then break; fi
      sleep 1
    done
    "$adb" shell am force-stop com.synchroconsumer
    if [ "$resumed" -ne 1 ]; then
      printf '%s\n' "Packaged React Native Android resume phase did not pass" >&2
      exit 1
    fi
    python3 "$tool" phase-result --output "$work_dir/resume.json" --phase resume --pid "$resume_pid" --pending-count 0
    native_artifact="$maven_dir/fit/trainstar/synchro/$version/synchro-$version.aar"
    ;;
esac

set -- python3 "$tool" complete-cell \
  --repo-root "$repo_root" \
  --cell "$cell_id" \
  --output "$cell_result" \
  --initial "$work_dir/initial.json" \
  --resume "$work_dir/resume.json" \
  --killed-pid "$initial_pid" \
  --artifact "$tarball" \
  --artifact "$native_artifact"
if [ -n "${PACKAGED_SMOKE_EXTRA_ARTIFACT:-}" ]; then
  set -- "$@" --artifact "$PACKAGED_SMOKE_EXTRA_ARTIFACT"
fi
"$@"

printf '%s\n' "Packaged React Native $platform smoke passed for $cell_id"
