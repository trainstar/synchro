#!/bin/sh
# Runs one upgrade phase for conformance/upgrade on one Android device. The
# candidate APK replaces the predecessor APK with `adb install -r`, which
# keeps the application data directory and its database.
set -eu

phase=${1:?phase is required}
adb=${ANDROID_HOME:?ANDROID_HOME is required}/platform-tools/adb
serial=${ANDROID_SERIAL:?ANDROID_SERIAL is required}
package=${SYNCHRO_UPGRADE_ANDROID_PACKAGE:?SYNCHRO_UPGRADE_ANDROID_PACKAGE is required}
activity=${SYNCHRO_UPGRADE_ANDROID_ACTIVITY:?SYNCHRO_UPGRADE_ANDROID_ACTIVITY is required}
control_url=${SYNCHRO_UPGRADE_CONTROL_URL:?SYNCHRO_UPGRADE_CONTROL_URL is required}
done_file=${SYNCHRO_UPGRADE_DONE_FILE:?SYNCHRO_UPGRADE_DONE_FILE is required}
case "$phase" in
  predecessor) apk=${SYNCHRO_UPGRADE_PREDECESSOR_APK:?SYNCHRO_UPGRADE_PREDECESSOR_APK is required}; version_code=1 ;;
  candidate) apk=${SYNCHRO_UPGRADE_CANDIDATE_APK:?SYNCHRO_UPGRADE_CANDIDATE_APK is required}; version_code=2 ;;
  *) printf '%s\n' "unknown upgrade phase: $phase" >&2; exit 1 ;;
esac
test -f "$apk"

device() { "$adb" -s "$serial" "$@"; }
port() { python3 -c 'import sys, urllib.parse; print(urllib.parse.urlsplit(sys.argv[1]).port)' "$1"; }

# The device reaches the host control server and adapter through loopback.
for url in "$control_url" "${SYNCHRO_TEST_URL:?SYNCHRO_TEST_URL is required}"; do
  forwarded=$(port "$url")
  device reverse "tcp:$forwarded" "tcp:$forwarded" >/dev/null
done

if [ "$phase" = predecessor ]; then
  device uninstall "$package" >/dev/null 2>&1 || true
  device install "$apk" >/dev/null
else
  device install -r "$apk" >/dev/null
fi
installed=$(device shell dumpsys package "$package" | tr -d '\r' | sed -n 's/^ *versionCode=\([0-9]*\).*/\1/p' | head -n 1)
if [ "$installed" != "$version_code" ]; then
  printf '%s\n' "installed $package versionCode is $installed, want $version_code" >&2
  exit 1
fi

device logcat -c
device shell am start -n "$package/$activity" --es control_url "$control_url" >/dev/null
pid=
for _ in $(seq 1 30); do
  pid=$(device shell pidof "$package" 2>/dev/null | tr -d '\r' || true)
  [ -n "$pid" ] && break
  sleep 1
done
case "$pid" in *[!0-9]*|'') printf '%s\n' "$package did not start" >&2; exit 1 ;; esac

status=1
for _ in $(seq 1 300); do
  if [ -f "$done_file" ]; then status=0; break; fi
  if [ "$(device shell pidof "$package" 2>/dev/null | tr -d '\r' || true)" != "$pid" ]; then break; fi
  sleep 1
done
if [ "$status" -ne 0 ]; then
  device logcat -d -t 200 AndroidRuntime:E System.err:W "*:S" >&2 || true
  printf '%s\n' "$phase application exited or timed out before its result" >&2
fi
device shell am force-stop "$package"
exit "$status"
