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
work_dir=$(mktemp -d "${TMPDIR:-/tmp}/synchro-swift-ios-consumer.XXXXXX")
trap 'rm -rf "$work_dir"' EXIT HUP INT TERM

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

bundle_id=dev.synchro.swift-consumer
app_path="$work_dir/derived-data/Build/Products/Debug-iphonesimulator/SynchroConsumer.app"
test -d "$app_path"
xcrun simctl install "$simulator_udid" "$app_path"
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
