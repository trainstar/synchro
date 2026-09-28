#!/bin/sh
set -eu

GIT_CONFIG_COUNT=1
GIT_CONFIG_KEY_0=safe.bareRepository
GIT_CONFIG_VALUE_0=all
export GIT_CONFIG_COUNT GIT_CONFIG_KEY_0 GIT_CONFIG_VALUE_0

repo_root=${1:?repository root is required}
artifact_dir=${2:?artifact directory is required}
cell_id=${3:?support cell id is required}
cell_result=${4:?cell result path is required}
tool="$repo_root/verification/packaged_smoke.py"
package="$artifact_dir/apple/Synchro"
archive="$artifact_dir/apple/synchro-spm-$(tr -d '\n' < "$repo_root/VERSION").tar.gz"
tmp_root=${PACKAGED_SMOKE_TMP_ROOT:?PACKAGED_SMOKE_TMP_ROOT is required}
phase_seconds=${PACKAGED_SMOKE_PHASE_SECONDS:-120}
case "$phase_seconds" in
  ''|*[!0-9]*|0) printf '%s\n' "PACKAGED_SMOKE_PHASE_SECONDS must be a positive integer" >&2; exit 1 ;;
esac

test -f "$package/Package.swift"
test -f "$archive"
mkdir -p "$tmp_root"
work_dir=$(mktemp -d "$tmp_root/swift-packaged-smoke.XXXXXX")
phase_pid=
cleanup() {
  status=$?
  trap - EXIT HUP INT TERM
  # Each phase ends at its own alarm, so this wait is bounded. The consumer
  # is gone before its files are removed.
  if [ -n "$phase_pid" ]; then
    wait "$phase_pid" 2>/dev/null || :
  fi
  rm -rf "$work_dir"
  exit "$status"
}
trap cleanup EXIT
trap 'exit 1' HUP INT TERM

# The shell reaps an exited child at once, so a later signal to its numeric
# process ID can reach an unrelated process. The consumer therefore gets a
# lifetime bound from an alarm that survives exec, and no cleanup signal.
start_phase() {
  SYNCHRO_PACKAGED_SMOKE_CONFIG="$work_dir/config.json" \
  SYNCHRO_PACKAGED_SMOKE_DATABASE="$work_dir/consumer.db" \
  SYNCHRO_PACKAGED_SMOKE_PHASE=$1 \
  SYNCHRO_PACKAGED_SMOKE_PHASE_RESULT="$work_dir/$1.json" \
    perl -e 'alarm shift @ARGV; exec { $ARGV[0] } @ARGV or exit 127' \
      "$phase_seconds" "$binary" > "$work_dir/$1.log" 2>&1 &
  phase_pid=$!
}

await_phase_result() {
  phase_ready=0
  elapsed=0
  while [ "$elapsed" -lt "$phase_seconds" ]; do
    if [ -f "$work_dir/$1.json" ]; then
      phase_ready=1
      return
    fi
    # Signal 0 only probes. The alarm bounds the consumer if this probe errs.
    if ! kill -0 "$phase_pid" 2>/dev/null; then
      return
    fi
    sleep 1
    elapsed=$((elapsed + 1))
  done
}

reap_phase() {
  set +e
  wait "$phase_pid"
  phase_status=$?
  set -e
  phase_pid=
}

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

start_phase initial
initial_pid=$phase_pid
await_phase_result initial
if [ "$phase_ready" -ne 1 ]; then
  reap_phase
  # The consumer names its failure on stderr, and the work directory is
  # deleted on exit, so the cause must be reported here or it is lost.
  cat "$work_dir/initial.log" >&2 || true
  printf '%s\n' "Packaged Swift initial phase did not become ready" >&2
  exit 1
fi

kill -9 "$initial_pid"
reap_phase
if [ "$phase_status" -ne 137 ]; then
  printf '%s\n' "Packaged Swift process kill was not observed" >&2
  exit 1
fi

start_phase resume
await_phase_result resume
reap_phase
if [ "$phase_ready" -ne 1 ] || [ "$phase_status" -ne 0 ]; then
  cat "$work_dir/resume.log" >&2 || true
  printf '%s\n' "Packaged Swift resume phase did not pass" >&2
  exit 1
fi

set -- python3 "$tool" complete-cell \
  --repo-root "$repo_root" \
  --cell "$cell_id" \
  --output "$cell_result" \
  --initial "$work_dir/initial.json" \
  --resume "$work_dir/resume.json" \
  --killed-pid "$initial_pid"
distribution_artifacts=${PACKAGED_SMOKE_DISTRIBUTION_ARTIFACTS:-$archive}
for artifact in $distribution_artifacts; do
  set -- "$@" --artifact "$artifact"
done
if [ -n "${PACKAGED_SMOKE_EXTRA_ARTIFACT:-}" ]; then
  set -- "$@" --artifact "$PACKAGED_SMOKE_EXTRA_ARTIFACT"
fi
for expected_hash in ${PACKAGED_SMOKE_EXPECTED_ARTIFACT_HASHES:?PACKAGED_SMOKE_EXPECTED_ARTIFACT_HASHES is required}; do
  set -- "$@" --expected-artifact-hash "$expected_hash"
done
"$@"

printf '%s\n' "Packaged Swift smoke passed for $cell_id"
