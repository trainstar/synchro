#!/bin/sh
set -eu

artifact_dir=${1:?artifact directory is required}
apple_package="$artifact_dir/apple/Synchro"

if [ ! -f "$apple_package/Package.swift" ]; then
  printf '%s\n' "Packaged Swift artifact is missing: $apple_package/Package.swift" >&2
  exit 1
fi

artifact_dir=$(cd "$artifact_dir" && pwd -P)
apple_package="$artifact_dir/apple/Synchro"
source_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)
repo_root=$(CDPATH= cd -- "$source_dir/../../.." && pwd -P)
tmp_root=${PACKAGED_SMOKE_TMP_ROOT:-$repo_root/.ignore/r2/tmp}
mkdir -p "$tmp_root"
work_dir=$(mktemp -d "$tmp_root/synchro-swift-ios-consumer.XXXXXX")
simulator_udid=
app_installed=0
bundle_id=dev.synchro.swift-consumer
cleanup() {
  if [ "$app_installed" -eq 1 ] && [ -n "$simulator_udid" ]; then
    xcrun simctl uninstall "$simulator_udid" "$bundle_id" >/dev/null 2>&1 || true
  fi
  rm -rf "$work_dir"
}
trap cleanup EXIT HUP INT TERM

cp -R "$source_dir" "$work_dir/app"
mkdir -p "$work_dir/app/Packages"
ln -s "$apple_package" "$work_dir/app/Packages/Synchro"

if [ -n "${IOS_SIMULATOR_UDID:-}" ]; then
  simulator_udid=$IOS_SIMULATOR_UDID
else
  simulator_udid=$(xcrun simctl list devices booted -j | ruby -rjson -e '
    devices = JSON.parse(STDIN.read).fetch("devices").values.flatten
    device = devices.find { |item| item["state"] == "Booted" }
    abort "no booted iOS simulator" unless device
    puts device.fetch("udid")
  ')
fi

simulator_version=$(xcrun simctl list devices booted -j | ruby -rjson -e '
  udid = ARGV.fetch(0)
  JSON.parse(STDIN.read).fetch("devices").each do |runtime, devices|
    if devices.any? { |device| device["udid"] == udid && device["state"] == "Booted" }
      puts runtime.sub(/^.*\.iOS-/, "").tr("-", ".")
      exit
    end
  end
  abort "the selected iOS simulator is not booted"
' "$simulator_udid")

if [ -n "${SUPPORT_PLATFORM_VERSION:-}" ]; then
  case "$SUPPORT_PLATFORM_VERSION" in
    *.*) actual_version=$simulator_version ;;
    *) actual_version=${simulator_version%%.*} ;;
  esac
  if [ "$actual_version" != "$SUPPORT_PLATFORM_VERSION" ]; then
    printf '%s\n' "iOS simulator version $simulator_version does not match SUPPORT_PLATFORM_VERSION $SUPPORT_PLATFORM_VERSION" >&2
    exit 1
  fi
fi

xcodebuild \
  -project "$work_dir/app/SynchroConsumer.xcodeproj" \
  -scheme SynchroConsumer \
  -configuration Debug \
  -sdk iphonesimulator \
  -destination "platform=iOS Simulator,id=$simulator_udid" \
  -derivedDataPath "$work_dir/derived-data" \
  IPHONEOS_DEPLOYMENT_TARGET=16.0 \
  CODE_SIGNING_ALLOWED=NO \
  build

app_path="$work_dir/derived-data/Build/Products/Debug-iphonesimulator/SynchroConsumer.app"
test -d "$app_path"
xcrun simctl uninstall "$simulator_udid" "$bundle_id" >/dev/null 2>&1 || true
xcrun simctl install "$simulator_udid" "$app_path"
app_installed=1

if [ -n "${PACKAGED_SMOKE_CELL_ID:-}" ]; then
  cell_result=${PACKAGED_SMOKE_CELL_RESULT:?PACKAGED_SMOKE_CELL_RESULT is required}
  tool="$repo_root/verification/packaged_smoke.py"
  archive="$artifact_dir/apple/synchro-spm-$(tr -d '\n' < "$repo_root/VERSION").tar.gz"
  test -f "$archive"
  python3 "$tool" config \
    --cell "$PACKAGED_SMOKE_CELL_ID" \
    --platform ios \
    --output "$work_dir/initial-config.json"
  python3 "$tool" set-config-phase \
    --config "$work_dir/initial-config.json" \
    --phase resume \
    --output "$work_dir/resume-config.json"

  container=$(xcrun simctl get_app_container "$simulator_udid" "$bundle_id" data)
  cp "$work_dir/initial-config.json" "$container/Documents/packaged-smoke-config.json"
  launch_output=$(xcrun simctl launch "$simulator_udid" "$bundle_id")
  initial_pid=${launch_output##*: }
  case "$initial_pid" in *[!0-9]*|'') printf '%s\n' "iOS initial process id is invalid" >&2; exit 1 ;; esac

  ready=0
  for _ in $(seq 1 120); do
    if [ -f "$container/Documents/initial-result.json" ]; then
      ready=1
      break
    fi
    if ! kill -0 "$initial_pid" >/dev/null 2>&1; then
      break
    fi
    sleep 1
  done
  if [ "$ready" -ne 1 ]; then
    # The app names its failure in the simulator log, and the phase leaves no
    # other trace, so the recent app log must be reported here or it is lost.
    xcrun simctl spawn "$simulator_udid" log show --last 3m --style compact \
      --predicate 'processImagePath CONTAINS "SynchroConsumer"' 2>/dev/null \
      | grep -vE "com.apple" | tail -40 >&2 || true
    printf '%s\n' "Packaged Swift iOS initial phase did not become ready" >&2
    exit 1
  fi

  kill -9 "$initial_pid"
  killed=0
  for _ in $(seq 1 30); do
    if ! kill -0 "$initial_pid" >/dev/null 2>&1; then
      killed=1
      break
    fi
    sleep 1
  done
  if [ "$killed" -ne 1 ]; then
    printf '%s\n' "Packaged Swift iOS process kill was not observed" >&2
    exit 1
  fi

  cp "$work_dir/resume-config.json" "$container/Documents/packaged-smoke-config.json"
  launch_output=$(xcrun simctl launch "$simulator_udid" "$bundle_id")
  resume_pid=${launch_output##*: }
  case "$resume_pid" in *[!0-9]*|'') printf '%s\n' "iOS resume process id is invalid" >&2; exit 1 ;; esac
  if [ "$resume_pid" = "$initial_pid" ]; then
    printf '%s\n' "Packaged Swift iOS resume reused the killed process" >&2
    exit 1
  fi

  resumed=0
  for _ in $(seq 1 120); do
    if [ -f "$container/Documents/resume-result.json" ]; then
      resumed=1
      break
    fi
    if ! kill -0 "$resume_pid" >/dev/null 2>&1; then
      break
    fi
    sleep 1
  done
  xcrun simctl terminate "$simulator_udid" "$bundle_id" >/dev/null 2>&1 || true
  if [ "$resumed" -ne 1 ]; then
    printf '%s\n' "Packaged Swift iOS resume phase did not pass" >&2
    exit 1
  fi

  cp "$container/Documents/initial-result.json" "$work_dir/initial.json"
  cp "$container/Documents/resume-result.json" "$work_dir/resume.json"
  set -- python3 "$tool" complete-cell \
    --repo-root "$repo_root" \
    --cell "$PACKAGED_SMOKE_CELL_ID" \
    --output "$cell_result" \
    --initial "$work_dir/initial.json" \
    --resume "$work_dir/resume.json" \
    --killed-pid "$initial_pid" \
    --artifact "$archive"
  if [ -n "${PACKAGED_SMOKE_EXTRA_ARTIFACT:-}" ]; then
    set -- "$@" --artifact "$PACKAGED_SMOKE_EXTRA_ARTIFACT"
  fi
  "$@"
  printf '%s\n' "Packaged Swift iOS smoke passed for $PACKAGED_SMOKE_CELL_ID"
  exit 0
fi

xcrun simctl launch "$simulator_udid" "$bundle_id" >/dev/null

passed=0
for _ in $(seq 1 60); do
  container=$(xcrun simctl get_app_container "$simulator_udid" "$bundle_id" data 2>/dev/null || true)
  if [ -n "$container" ] && [ -f "$container/Documents/consumer.db" ]; then
    value=$(sqlite3 "$container/Documents/consumer.db" "SELECT value FROM consumer_probe WHERE id = 'probe';" 2>/dev/null || true)
    if [ "$value" = "packaged" ]; then
      passed=1
      break
    fi
  fi
  sleep 1
done

if [ "$passed" -ne 1 ]; then
  printf '%s\n' "Packaged Swift iOS consumer did not write its SQLite canary" >&2
  exit 1
fi

printf '%s\n' "Packaged Swift iOS consumer passed"
