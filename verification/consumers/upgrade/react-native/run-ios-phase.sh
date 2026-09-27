#!/bin/sh
# Runs one upgrade phase for conformance/upgrade on one iOS simulator. The
# candidate application installs over the predecessor, which keeps the
# application data container and its database.
set -eu

phase=${1:?phase is required}
udid=${SYNCHRO_UPGRADE_IOS_SIMULATOR:?SYNCHRO_UPGRADE_IOS_SIMULATOR is required}
bundle=${SYNCHRO_UPGRADE_IOS_BUNDLE:?SYNCHRO_UPGRADE_IOS_BUNDLE is required}
work=${SYNCHRO_UPGRADE_WORK_DIR:?SYNCHRO_UPGRADE_WORK_DIR is required}
done_file=${SYNCHRO_UPGRADE_DONE_FILE:?SYNCHRO_UPGRADE_DONE_FILE is required}
case "$phase" in
  predecessor|candidate) app=$work/$phase.app ;;
  *) printf '%s\n' "unknown upgrade phase: $phase" >&2; exit 1 ;;
esac
test -d "$app"

if [ "$phase" = predecessor ]; then
  xcrun simctl uninstall "$udid" "$bundle" >/dev/null 2>&1 || true
fi
xcrun simctl install "$udid" "$app"
container=$(xcrun simctl get_app_container "$udid" "$bundle" data)
if [ "$phase" = predecessor ]; then
  printf '%s\n' "$container" > "$work/data-container"
elif [ "$container" != "$(cat "$work/data-container")" ]; then
  printf '%s\n' "the candidate install replaced the predecessor data container" >&2
  exit 1
fi

# The simulator launch service can refuse a request on a freshly booted
# device, so the launch retries before it fails.
attempt=1
until launch=$(xcrun simctl launch "$udid" "$bundle"); do
  if [ "$attempt" -ge 3 ]; then exit 1; fi
  attempt=$((attempt + 1))
  sleep 15
done
pid=${launch##*: }
case "$pid" in *[!0-9]*|'') printf '%s\n' "$bundle launch returned no process id" >&2; exit 1 ;; esac

status=1
for _ in $(seq 1 300); do
  if [ -f "$done_file" ]; then status=0; break; fi
  if ! kill -0 "$pid" 2>/dev/null; then break; fi
  sleep 1
done
if [ "$status" -ne 0 ]; then
  xcrun simctl spawn "$udid" log show --last 5m --style compact \
    --predicate 'processImagePath CONTAINS "SynchroUpgrade"' 2>/dev/null | tail -60 >&2 || true
  printf '%s\n' "$phase application exited or timed out before its result" >&2
fi
xcrun simctl terminate "$udid" "$bundle" >/dev/null 2>&1 || true
exit "$status"
