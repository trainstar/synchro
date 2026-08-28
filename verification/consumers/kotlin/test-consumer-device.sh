#!/bin/sh
set -eu

repo_root=${1:?repository root is required}
artifact_dir=${2:?artifact directory is required}
cell_id=${3:?support cell id is required}
cell_result=${4:?cell result path is required}
version=${5:?version is required}
tool="$repo_root/verification/packaged_smoke.py"
tmp_root=${PACKAGED_SMOKE_TMP_ROOT:?PACKAGED_SMOKE_TMP_ROOT is required}
adb=${ANDROID_HOME:?ANDROID_HOME is required}/platform-tools/adb
serial=${KOTLIN_ANDROID_SERIAL:-${ANDROID_SERIAL:-}}
package=com.trainstar.synchro.consumer
apk="$repo_root/verification/consumers/kotlin/app/build/outputs/apk/debug/app-debug.apk"
aar="$artifact_dir/maven/fit/trainstar/synchro/$version/synchro-$version.aar"

adb_command() {
  if [ -n "$serial" ]; then
    "$adb" -s "$serial" "$@"
  else
    "$adb" "$@"
  fi
}

test -x "$adb"
test -f "$apk"
test -f "$aar"
mkdir -p "$tmp_root"
work_dir=$(mktemp -d "$tmp_root/kotlin-packaged-smoke.XXXXXX")
app_installed=0
reverse_port=
cleanup() {
  if [ "$app_installed" -eq 1 ]; then
    adb_command shell am force-stop "$package" >/dev/null 2>&1 || true
    adb_command uninstall "$package" >/dev/null 2>&1 || true
  fi
  if [ -n "$reverse_port" ]; then
    adb_command reverse --remove "tcp:$reverse_port" >/dev/null 2>&1 || true
  fi
  rm -rf "$work_dir"
}
trap cleanup EXIT HUP INT TERM

python3 "$tool" config \
  --cell "$cell_id" \
  --platform android \
  --output "$work_dir/initial-config.json"
python3 "$tool" set-config-phase \
  --config "$work_dir/initial-config.json" \
  --phase resume \
  --output "$work_dir/resume-config.json"

server_url=$(python3 "$tool" config-value --config "$work_dir/initial-config.json" --field server_url)
reverse_port=$(python3 -c 'import sys, urllib.parse; value=urllib.parse.urlsplit(sys.argv[1]); print(value.port or (443 if value.scheme == "https" else 80)) if value.hostname in {"127.0.0.1", "localhost"} else None' "$server_url")
if [ -n "$reverse_port" ]; then
  adb_command reverse "tcp:$reverse_port" "tcp:$reverse_port"
fi

adb_command uninstall "$package" >/dev/null 2>&1 || true
adb_command install "$apk" >/dev/null
app_installed=1
adb_command shell pm clear "$package" >/dev/null

write_config() {
  source_path=$1
  remote_path="/data/local/tmp/synchro-packaged-smoke-$$.json"
  adb_command push "$source_path" "$remote_path" >/dev/null
  adb_command shell run-as "$package" cp "$remote_path" files/packaged-smoke-config.json
  adb_command shell rm -f "$remote_path"
}

write_config "$work_dir/initial-config.json"
adb_command shell am start -W -n "$package/.MainActivity" >/dev/null
initial_pid=$(adb_command shell pidof "$package" | tr -d '\r')
case "$initial_pid" in *[!0-9]*|'') printf '%s\n' "Android initial process id is invalid" >&2; exit 1 ;; esac

ready=0
for _ in $(seq 1 120); do
  if adb_command exec-out run-as "$package" cat files/initial-result.json > "$work_dir/initial.json" 2>/dev/null; then
    ready=1
    break
  fi
  if ! adb_command shell run-as "$package" kill -0 "$initial_pid" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
if [ "$ready" -ne 1 ]; then
  printf '%s\n' "Packaged Kotlin initial phase did not become ready" >&2
  exit 1
fi

set +e
adb_command shell run-as "$package" kill -9 "$initial_pid"
kill_status=$?
set -e
case "$kill_status" in
  0|137) ;;
  *) printf '%s\n' "Android kill command failed" >&2; exit 1 ;;
esac
killed=0
for _ in $(seq 1 30); do
  current_pid=$(adb_command shell pidof "$package" 2>/dev/null | tr -d '\r' || true)
  if [ "$current_pid" != "$initial_pid" ]; then
    killed=1
    break
  fi
  sleep 1
done
if [ "$killed" -ne 1 ]; then
  printf '%s\n' "Packaged Kotlin process kill was not observed" >&2
  exit 1
fi

write_config "$work_dir/resume-config.json"
adb_command shell am start -W -n "$package/.MainActivity" >/dev/null
resume_pid=$(adb_command shell pidof "$package" | tr -d '\r')
case "$resume_pid" in *[!0-9]*|'') printf '%s\n' "Android resume process id is invalid" >&2; exit 1 ;; esac
if [ "$resume_pid" = "$initial_pid" ]; then
  printf '%s\n' "Packaged Kotlin resume reused the killed process" >&2
  exit 1
fi

resumed=0
for _ in $(seq 1 120); do
  if adb_command exec-out run-as "$package" cat files/resume-result.json > "$work_dir/resume.json" 2>/dev/null; then
    resumed=1
    break
  fi
  if ! adb_command shell run-as "$package" kill -0 "$resume_pid" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
adb_command shell am force-stop "$package"
if [ "$resumed" -ne 1 ]; then
  printf '%s\n' "Packaged Kotlin resume phase did not pass" >&2
  exit 1
fi

set -- python3 "$tool" complete-cell \
  --repo-root "$repo_root" \
  --cell "$cell_id" \
  --output "$cell_result" \
  --initial "$work_dir/initial.json" \
  --resume "$work_dir/resume.json" \
  --killed-pid "$initial_pid" \
  --artifact "$aar"
if [ -n "${PACKAGED_SMOKE_EXTRA_ARTIFACT:-}" ]; then
  set -- "$@" --artifact "$PACKAGED_SMOKE_EXTRA_ARTIFACT"
fi
"$@"

printf '%s\n' "Packaged Kotlin smoke passed for $cell_id"
