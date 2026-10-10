#!/bin/sh
set -eu

repo_root=${1:?repository root is required}
artifact_dir=${2:?artifact directory is required}
cell_id=${3:?support cell id is required}
cell_result=${4:?cell result path is required}
version=${5:?version is required}
tool="$repo_root/verification/packaged_smoke.py"
probe="$repo_root/verification/probe_support_environment.py"
environment_dir="$cell_result.environments"
release_manifest=${PACKAGED_SMOKE_RELEASE_MANIFEST:?PACKAGED_SMOKE_RELEASE_MANIFEST is required}
test -f "$release_manifest" && test -r "$release_manifest" || { printf '%s\n' "PACKAGED_SMOKE_RELEASE_MANIFEST must be a readable file" >&2; exit 1; }
tmp_root=${PACKAGED_SMOKE_TMP_ROOT:?PACKAGED_SMOKE_TMP_ROOT is required}
adb=${ANDROID_HOME:?ANDROID_HOME is required}/platform-tools/adb
package=com.trainstar.synchro.consumer
apk="$repo_root/verification/consumers/kotlin/app/build/outputs/apk/debug/app-debug.apk"
aar="$artifact_dir/maven/fit/trainstar/synchro/$version/synchro-$version.aar"

adb_command() {
  python3 -c '
import subprocess
import sys

try:
    status = subprocess.run(sys.argv[1:], timeout=180).returncode
except subprocess.TimeoutExpired:
    print("Android ADB client deadline exceeded", file=sys.stderr)
    sys.exit(124)
except OSError:
    print("Android ADB client invocation failed", file=sys.stderr)
    sys.exit(127)
sys.exit(status if status >= 0 else 128 - status)
' "$adb" -L tcp:127.0.0.1:5037 -s "$serial" "$@"
}

test -x "$adb"
test -f "$apk"
test -f "$aar"
serial=$(python3 "$probe" resolve-android-serial --sdk-root "$ANDROID_HOME")
ANDROID_SERIAL=$serial
KOTLIN_ANDROID_SERIAL=$serial
export ANDROID_SERIAL KOTLIN_ANDROID_SERIAL
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
  # The files directory does not exist before the app first launches.
  adb_command shell run-as "$package" mkdir -p files
  adb_command shell run-as "$package" cp "$remote_path" files/packaged-smoke-config.json
  adb_command shell rm -f "$remote_path"
}

write_config "$work_dir/initial-config.json"
python3 "$probe" android --cell "$cell_id" --sdk-root "$ANDROID_HOME" --serial "$serial" \
  --output "$environment_dir/initial.json" --identity-output "$environment_dir/initial-identity.json"
adb_command shell am start -n "$package/.MainActivity" >/dev/null
initial_pid=""
for _ in $(seq 1 30); do
  initial_pid=$(adb_command shell pidof "$package" 2>/dev/null | tr -d '\r' || true)
  [ -n "$initial_pid" ] && break
  sleep 1
done
case "$initial_pid" in *[!0-9]*|'') printf '%s\n' "Android initial process id is invalid" >&2; exit 1 ;; esac

ready=0
for _ in $(seq 1 120); do
  # exec-out reports success even when the remote cat fails, so only the
  # captured content proves the phase result exists.
  if adb_command exec-out run-as "$package" cat files/initial-result.json > "$work_dir/initial.json" 2> "$work_dir/initial-result.stderr"; then
    initial_result_status=0
  else
    initial_result_status=$?
  fi
  if grep -q '"phase"' "$work_dir/initial.json" 2>/dev/null; then
    ready=1
    break
  fi
  # kill -0 through run-as is permission-denied against a live process on
  # API 34, so liveness is the process id still being listed.
  if adb_command shell pidof "$package" > "$work_dir/initial-liveness.stdout" 2> "$work_dir/initial-liveness.stderr"; then
    current_pid_status=0
  else
    current_pid_status=$?
  fi
  current_pid=$(tr -d '\r' < "$work_dir/initial-liveness.stdout")
  if [ "$current_pid" != "$initial_pid" ]; then
    break
  fi
  sleep 1
done
if [ "$ready" -ne 1 ]; then
  if adb_command logcat -d -t 120 AndroidRuntime:E "*:S" > "$work_dir/initial-runtime.log" 2> "$work_dir/initial-runtime.stderr"; then
    runtime_log_status=0
  else
    runtime_log_status=$?
  fi
  # Transport output and exception messages can contain application data.
  python3 - "$work_dir" "$environment_dir/initial-readiness-failure.json" \
    "$initial_pid" "$initial_result_status" "$current_pid_status" "$runtime_log_status" <<'PY'
import json
from pathlib import Path
import re
import sys

work = Path(sys.argv[1])
def bounded_text(name):
    path = work / name
    with path.open("rb") as stream:
        return stream.read(8192).decode("utf-8", errors="replace")

def pid(value):
    return int(value) if re.fullmatch(r"[0-9]{1,20}", value) else None

result_text = bounded_text("initial.json")
try:
    result = json.loads(result_text)
except (ValueError, RecursionError):
    result = None
runtime_log = bounded_text("initial-runtime.log")
report = {
    "expected_pid": pid(sys.argv[3]),
    "observed_pid": pid(bounded_text("initial-liveness.stdout").replace("\r", "").rstrip("\n")),
    "initial_result_returncode": int(sys.argv[4]),
    "liveness_returncode": int(sys.argv[5]),
    "runtime_log_returncode": int(sys.argv[6]),
    "initial_result_bytes": len(result_text.encode("utf-8")),
    "android_runtime_fatal_exception": "FATAL EXCEPTION" in runtime_log,
    "exception_classes": sorted(set(re.findall(
        r"\b(?:java|android|kotlin|kotlinx|com\.trainstar\.synchro)\.[A-Za-z0-9_.$]*(?:Exception|Error)(?:\$[A-Za-z_][A-Za-z_0-9]*)?\b",
        runtime_log,
    )))[:8],
}
if isinstance(result, dict):
    report["initial_result_phase"] = result.get("phase") if result.get("phase") in ("initial", "resume") else None
    report["initial_result_status"] = result.get("status") if result.get("status") in ("passed", "failed") else None
output = Path(sys.argv[2])
output.parent.mkdir(parents=True, exist_ok=True)
output.write_text(json.dumps(report, indent=2) + "\n")
print(json.dumps(report), file=sys.stderr)
PY
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

# Android restarts a killed foreground activity within a second, and that
# uncontrolled instance races the resume configuration swap. The stop clears
# it so the resume launch starts from a controlled state.
adb_command shell am force-stop "$package"

python3 "$tool" author-remote --config "$work_dir/initial-config.json" --output "$work_dir/remote.json"
write_config "$work_dir/resume-config.json"
# am start -W never returns when the launched activity dies at once, so the
# launch is asynchronous and the process id is polled.
python3 "$probe" android --cell "$cell_id" --sdk-root "$ANDROID_HOME" --serial "$serial" \
  --output "$environment_dir/resume.json" --identity-output "$environment_dir/resume-identity.json" \
  --initial-identity "$environment_dir/initial-identity.json" --initial-environment "$environment_dir/initial.json"
adb_command shell am start -n "$package/.MainActivity" >/dev/null
resume_pid=""
for _ in $(seq 1 30); do
  resume_pid=$(adb_command shell pidof "$package" 2>/dev/null | tr -d '\r' || true)
  [ -n "$resume_pid" ] && break
  sleep 1
done
case "$resume_pid" in *[!0-9]*|'') printf '%s\n' "Android resume process id is invalid" >&2; exit 1 ;; esac
if [ "$resume_pid" = "$initial_pid" ]; then
  printf '%s\n' "Packaged Kotlin resume reused the killed process" >&2
  exit 1
fi

resumed=0
for _ in $(seq 1 120); do
  adb_command exec-out run-as "$package" cat files/resume-result.json > "$work_dir/resume.json" 2>/dev/null || true
  if grep -q '"phase"' "$work_dir/resume.json" 2>/dev/null; then
    resumed=1
    break
  fi
  current_pid=$(adb_command shell pidof "$package" 2>/dev/null | tr -d '\r' || true)
  if [ "$current_pid" != "$resume_pid" ]; then
    break
  fi
  sleep 1
done
adb_command shell am force-stop "$package"
if [ "$resumed" -ne 1 ]; then
  # The app names its failure in logcat and nothing else records it.
  adb_command logcat -d -t 80 AndroidRuntime:E SynchroConsumer:V "*:S" >&2 || true
  printf '%s\n' "Packaged Kotlin resume phase did not pass" >&2
  exit 1
fi

python3 "$tool" verify-server --config "$work_dir/initial-config.json" --remote "$work_dir/remote.json" --output "$work_dir/server.json"
set -- python3 "$tool" complete-cell \
  --repo-root "$repo_root" \
  --cell "$cell_id" \
  --output "$cell_result" \
  --initial "$work_dir/initial.json" \
  --resume "$work_dir/resume.json" \
  --killed-pid "$initial_pid" \
  --remote "$work_dir/remote.json" \
  --server-verification "$work_dir/server.json" \
  --initial-environment "$environment_dir/initial.json" \
  --resume-environment "$environment_dir/resume.json" \
  --release-manifest "$release_manifest"
distribution_artifacts=${PACKAGED_SMOKE_DISTRIBUTION_ARTIFACTS:-$aar}
for artifact in $distribution_artifacts; do
  set -- "$@" --artifact "$artifact"
done
for expected_hash in ${PACKAGED_SMOKE_EXPECTED_ARTIFACT_HASHES:?PACKAGED_SMOKE_EXPECTED_ARTIFACT_HASHES is required}; do
  set -- "$@" --expected-artifact-hash "$expected_hash"
done
"$@"

printf '%s\n' "Packaged Kotlin smoke passed for $cell_id"
