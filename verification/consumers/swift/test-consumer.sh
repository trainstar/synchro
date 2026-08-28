#!/bin/sh
set -eu

repo_root=${1:?repository root is required}
artifact_dir=${2:?artifact directory is required}
cell_id=${3:?support cell id is required}
cell_result=${4:?cell result path is required}
tool="$repo_root/verification/packaged_smoke.py"
package="$artifact_dir/apple/Synchro"
archive="$artifact_dir/apple/synchro-spm-$(tr -d '\n' < "$repo_root/VERSION").tar.gz"
tmp_root=${PACKAGED_SMOKE_TMP_ROOT:?PACKAGED_SMOKE_TMP_ROOT is required}

test -f "$package/Package.swift"
test -f "$archive"
mkdir -p "$tmp_root"
work_dir=$(mktemp -d "$tmp_root/swift-packaged-smoke.XXXXXX")
trap 'rm -rf "$work_dir"' EXIT HUP INT TERM

python3 "$tool" config \
  --cell "$cell_id" \
  --platform macos \
  --output "$work_dir/config.json"

SYNCHRO_SWIFT_PACKAGE_PATH="$package" swift package \
  --package-path "$repo_root/verification/consumers/swift" \
  --scratch-path "$work_dir/build" \
  show-dependencies --format json > "$work_dir/dependencies.json"
if grep -F "$repo_root/clients/swift" "$work_dir/dependencies.json" >/dev/null; then
  printf '%s\n' "Swift consumer resolved workspace client sources" >&2
  exit 1
fi
SYNCHRO_SWIFT_PACKAGE_PATH="$package" swift build \
  --package-path "$repo_root/verification/consumers/swift" \
  --scratch-path "$work_dir/build" \
  --product SynchroConsumer
binary="$work_dir/build/debug/SynchroConsumer"
test -x "$binary"

SYNCHRO_PACKAGED_SMOKE_CONFIG="$work_dir/config.json" \
SYNCHRO_PACKAGED_SMOKE_DATABASE="$work_dir/consumer.db" \
SYNCHRO_PACKAGED_SMOKE_PHASE=initial \
SYNCHRO_PACKAGED_SMOKE_PHASE_RESULT="$work_dir/initial.json" \
  "$binary" > "$work_dir/initial.log" 2>&1 &
initial_pid=$!

ready=0
for _ in $(seq 1 120); do
  if [ -f "$work_dir/initial.json" ]; then
    ready=1
    break
  fi
  if ! kill -0 "$initial_pid" 2>/dev/null; then
    break
  fi
  sleep 1
done
if [ "$ready" -ne 1 ]; then
  wait "$initial_pid" || true
  printf '%s\n' "Packaged Swift initial phase did not become ready" >&2
  exit 1
fi

kill -9 "$initial_pid"
set +e
wait "$initial_pid"
kill_status=$?
set -e
if [ "$kill_status" -ne 137 ] || kill -0 "$initial_pid" 2>/dev/null; then
  printf '%s\n' "Packaged Swift process kill was not observed" >&2
  exit 1
fi

SYNCHRO_PACKAGED_SMOKE_CONFIG="$work_dir/config.json" \
SYNCHRO_PACKAGED_SMOKE_DATABASE="$work_dir/consumer.db" \
SYNCHRO_PACKAGED_SMOKE_PHASE=resume \
SYNCHRO_PACKAGED_SMOKE_PHASE_RESULT="$work_dir/resume.json" \
  "$binary" > "$work_dir/resume.log" 2>&1 &
resume_pid=$!

resumed=0
for _ in $(seq 1 120); do
  if [ -f "$work_dir/resume.json" ]; then
    resumed=1
    break
  fi
  if ! kill -0 "$resume_pid" 2>/dev/null; then
    break
  fi
  sleep 1
done
wait "$resume_pid"
if [ "$resumed" -ne 1 ]; then
  printf '%s\n' "Packaged Swift resume phase did not pass" >&2
  exit 1
fi

set -- python3 "$tool" complete-cell \
  --repo-root "$repo_root" \
  --cell "$cell_id" \
  --output "$cell_result" \
  --initial "$work_dir/initial.json" \
  --resume "$work_dir/resume.json" \
  --killed-pid "$initial_pid" \
  --artifact "$archive"
if [ -n "${PACKAGED_SMOKE_EXTRA_ARTIFACT:-}" ]; then
  set -- "$@" --artifact "$PACKAGED_SMOKE_EXTRA_ARTIFACT"
fi
"$@"

printf '%s\n' "Packaged Swift smoke passed for $cell_id"
