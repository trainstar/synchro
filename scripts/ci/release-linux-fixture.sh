#!/bin/sh
# Run one Apple package cell against an isolated Linux release fixture.
set -eu

setup_usage() {
  printf '%s\n' "usage: $0 setup-ssh OUTPUT-DIRECTORY" >&2
  exit 2
}

setup_ssh() {
  [ "$#" -eq 1 ] || setup_usage
  output_dir=$1
  case "$output_dir" in /*) ;; *) printf '%s\n' "SSH output directory must be absolute" >&2; exit 1 ;; esac
  case "$output_dir" in *[!A-Za-z0-9_./-]*|*/../*|*/..|*//*|/) printf '%s\n' "SSH output directory is unsafe" >&2; exit 1 ;; esac
  : "${RELEASE_FIXTURE_SSH_PRIVATE_KEY:?RELEASE_FIXTURE_SSH_PRIVATE_KEY is required}"
  : "${RELEASE_FIXTURE_SSH_KNOWN_HOSTS:?RELEASE_FIXTURE_SSH_KNOWN_HOSTS is required}"
  [ ! -e "$output_dir" ] || { printf '%s\n' "SSH output directory already exists" >&2; exit 1; }
  umask 077
  mkdir -m 700 "$output_dir"
  printf '%s\n' "$RELEASE_FIXTURE_SSH_PRIVATE_KEY" > "$output_dir/private-key"
  printf '%s\n' "$RELEASE_FIXTURE_SSH_KNOWN_HOSTS" > "$output_dir/known-hosts"
  chmod 600 "$output_dir/private-key" "$output_dir/known-hosts"
}

select_free_port() {
  python3 - "$@" <<'PY'
import socket
import sys

excluded = set()
for raw in sys.argv[1:]:
    try:
        port = int(raw)
    except ValueError as error:
        raise SystemExit(f"excluded port is invalid: {raw}") from error
    if not 1 <= port <= 65535:
        raise SystemExit(f"excluded port is invalid: {raw}")
    excluded.add(port)

while True:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    if port not in excluded:
        print(port)
        break
PY
}

case "${1:-}" in
  setup-ssh)
    shift
    setup_ssh "$@"
    exit 0
    ;;
esac

usage() {
  printf '%s\n' "usage: $0 --known-hosts FILE --key FILE --user USER --host HOST --remote-root DIR --pg18-bin-dir DIR --release-dir DIR --version X.Y.Z --provisioner FILE --cell ID -- COMMAND..." >&2
  exit 2
}

known_hosts=
key_file=
ssh_user=
ssh_host=
remote_root=
pg18_bin_dir=
release_dir=
version=
provisioner=
cell=
while [ "$#" -gt 0 ]; do
  case "$1" in
    --known-hosts|--key|--user|--host|--remote-root|--pg18-bin-dir|--release-dir|--version|--provisioner|--cell)
      [ "$#" -ge 2 ] || usage
      case "$1" in
        --known-hosts) known_hosts=$2 ;;
        --key) key_file=$2 ;;
        --user) ssh_user=$2 ;;
        --host) ssh_host=$2 ;;
        --remote-root) remote_root=$2 ;;
        --pg18-bin-dir) pg18_bin_dir=$2 ;;
        --release-dir) release_dir=$2 ;;
        --version) version=$2 ;;
        --provisioner) provisioner=$2 ;;
        --cell) cell=$2 ;;
      esac
      shift 2
      ;;
    --) shift; break ;;
    *) usage ;;
  esac
done
[ "$#" -gt 0 ] || usage
[ -f "$known_hosts" ] && [ ! -L "$known_hosts" ] || { printf '%s\n' "known-hosts file is missing or unsafe" >&2; exit 1; }
[ -f "$key_file" ] && [ ! -L "$key_file" ] || { printf '%s\n' "SSH key file is missing or unsafe" >&2; exit 1; }
[ -d "$release_dir" ] && [ ! -L "$release_dir" ] || { printf '%s\n' "sealed release directory is missing or unsafe" >&2; exit 1; }
[ -f "$provisioner" ] && [ ! -L "$provisioner" ] || { printf '%s\n' "provisioner is missing or unsafe" >&2; exit 1; }
chmod 700 "$provisioner"
[ -x "$provisioner" ] || { printf '%s\n' "provisioner is not executable" >&2; exit 1; }
case "$version" in [0-9]*.[0-9]*.[0-9]*) ;; *) printf '%s\n' "release version is invalid" >&2; exit 1 ;; esac
case "$ssh_user" in ''|*[!A-Za-z0-9_.-]*) printf '%s\n' "SSH user is invalid" >&2; exit 1 ;; esac
case "$ssh_host" in ''|*[!A-Za-z0-9:._-]*) printf '%s\n' "SSH host is invalid" >&2; exit 1 ;; esac
case "$remote_root" in /*) ;; *) printf '%s\n' "remote root must be absolute" >&2; exit 1 ;; esac
case "$remote_root" in *[!A-Za-z0-9_./-]*|*/../*|*/..|*//*|/) printf '%s\n' "remote root is unsafe" >&2; exit 1 ;; esac
case "$pg18_bin_dir" in /*) ;; *) printf '%s\n' "PostgreSQL bindir must be absolute" >&2; exit 1 ;; esac
case "$pg18_bin_dir" in *[!A-Za-z0-9_./-]*|*/../*|*/..|*//*) printf '%s\n' "PostgreSQL bindir is unsafe" >&2; exit 1 ;; esac
case "$cell" in CI-SWIFT|CI-RN-IOS|SUP-IOS-MIN-001|SUP-IOS-CURRENT-001|SUP-RN-IOS-CURRENT-001) ;; *) printf '%s\n' "fixture cell is invalid" >&2; exit 1 ;; esac

: "${GITHUB_RUN_ID:?GITHUB_RUN_ID is required}"
: "${GITHUB_RUN_ATTEMPT:?GITHUB_RUN_ATTEMPT is required}"
: "${GITHUB_JOB:?GITHUB_JOB is required}"
case "$GITHUB_RUN_ID:$GITHUB_RUN_ATTEMPT:$GITHUB_JOB" in *[!A-Za-z0-9_.:-]*) printf '%s\n' "GitHub run identity is invalid" >&2; exit 1 ;; esac
run_name="run-${GITHUB_RUN_ID}-${GITHUB_RUN_ATTEMPT}-${GITHUB_JOB}-${cell}"
remote_run="$remote_root/$run_name"

work_dir=$(mktemp -d "${RUNNER_TEMP:-${TMPDIR:-/tmp}}/synchro-linux-fixture.XXXXXX")
forward_pid=
remote_created=0
run_id=

ssh_target="$ssh_user@$ssh_host"
ssh_command() {
  ssh -i "$key_file" -o BatchMode=yes -o IdentitiesOnly=yes -o StrictHostKeyChecking=yes -o "UserKnownHostsFile=$known_hosts" "$@"
}
scp_command() {
  scp -i "$key_file" -o BatchMode=yes -o IdentitiesOnly=yes -o StrictHostKeyChecking=yes -o "UserKnownHostsFile=$known_hosts" "$@"
}

remote_cleanup() {
  if [ -n "$run_id" ]; then
    cleanup_mode=attached
    cleanup_run_id=$run_id
  else
    cleanup_mode=pre-attach
    cleanup_run_id=-
  fi
  ssh_command "$ssh_target" sh -s -- "$remote_root" "$remote_run" "$run_name" "$cleanup_mode" "$cleanup_run_id" <<'REMOTE'
set -eu
root=$1
run=$2
name=$3
mode=$4
run_id=$5
[ "$run" = "$root/$name" ] || exit 1
[ "$(dirname "$run")" = "$root" ] || exit 1
[ "$(basename "$run")" = "$name" ] || exit 1
[ -d "$root" ] && [ ! -L "$root" ] && [ "$(cd "$root" && pwd -P)" = "$root" ] || exit 1
[ ! -e "$run" ] && exit 0
[ -d "$run" ] && [ ! -L "$run" ] && [ "$(cd "$run" && pwd -P)" = "$run" ] || exit 1
stop_process() {
  label=$1
  file=$2
  expected=$3
  [ -e "$file" ] || return 0
  [ -f "$file" ] && [ ! -L "$file" ] || { printf '%s\n' "$label pid file is unsafe" >&2; return 1; }
  pid=$(cat "$file")
  case "$pid" in *[!0-9]*|'') printf '%s\n' "$label pid is invalid" >&2; return 1 ;; esac
  kill -0 "$pid" >/dev/null 2>&1 || return 0
  [ -r "/proc/$pid/cmdline" ] || { printf '%s\n' "cannot verify $label process $pid" >&2; return 1; }
  process_command=$(tr '\000' '\n' < "/proc/$pid/cmdline")
  case "$process_command" in
    *"$expected"*) ;;
    *) printf '%s\n' "$label pid $pid does not belong to this fixture" >&2; return 1 ;;
  esac
  kill "$pid" || { printf '%s\n' "could not stop $label process $pid" >&2; return 1; }
  for unused in $(seq 1 30); do
    kill -0 "$pid" >/dev/null 2>&1 || return 0
    sleep 1
  done
  printf '%s\n' "$label process $pid did not stop" >&2
  return 1
}
confirm_process_shutdown() {
  label=$1
  file=$2
  expected=$3
  [ -f "$file" ] && [ ! -L "$file" ] || { printf '%s\n' "$label pid file is missing or unsafe" >&2; return 1; }
  pid=$(cat "$file")
  case "$pid" in *[!0-9]*|'') printf '%s\n' "$label pid is invalid" >&2; return 1 ;; esac
  for unused in $(seq 1 30); do
    kill -0 "$pid" >/dev/null 2>&1 || return 0
    [ -r "/proc/$pid/cmdline" ] || { printf '%s\n' "cannot verify $label process $pid" >&2; return 1; }
    process_command=$(tr '\000' '\n' < "/proc/$pid/cmdline")
    case "$process_command" in
      *"$expected"*) ;;
      *) printf '%s\n' "$label pid $pid does not belong to this fixture" >&2; return 1 ;;
    esac
    sleep 1
  done
  printf '%s\n' "$label process $pid did not stop" >&2
  return 1
}
read_lifecycle_state() {
  python3 - "$run/state/lifecycle-state.json" <<'PY'
import json
import os
import re
import stat
import sys

path = sys.argv[1]
directory = os.path.dirname(path)
try:
    directory_info = os.lstat(directory)
except FileNotFoundError:
    raise SystemExit(2)
if stat.S_ISLNK(directory_info.st_mode) or not stat.S_ISDIR(directory_info.st_mode) or stat.S_IMODE(directory_info.st_mode) & 0o077:
    raise SystemExit("lifecycle state directory is unsafe")
try:
    info = os.lstat(path)
except FileNotFoundError:
    raise SystemExit(2)
if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode) or stat.S_IMODE(info.st_mode) & 0o077:
    raise SystemExit("lifecycle state file is unsafe")
if info.st_size > 65536:
    raise SystemExit("lifecycle state file is too large")

def object_pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate lifecycle state member")
        result[key] = value
    return result

try:
    with open(path, "r", encoding="utf-8") as stream:
        value = json.load(stream, object_pairs_hook=object_pairs)
except (OSError, UnicodeError, ValueError, json.JSONDecodeError) as error:
    raise SystemExit(f"lifecycle state is invalid: {error}")
if not isinstance(value, dict) or set(value) != {"run_id", "control_address", "destroyed"}:
    raise SystemExit("lifecycle state members are invalid")
run_id = value["run_id"]
control_address = value["control_address"]
destroyed = value["destroyed"]
if not isinstance(run_id, str) or re.fullmatch(r"[0-9a-f]{32}", run_id) is None:
    raise SystemExit("lifecycle state run ID is invalid")
if type(destroyed) is not bool or not isinstance(control_address, str):
    raise SystemExit("lifecycle state values are invalid")
if destroyed:
    if control_address:
        raise SystemExit("destroyed lifecycle state has a control address")
else:
    match = re.fullmatch(r"127[.]0[.]0[.]1:([1-9][0-9]{0,4})", control_address)
    if match is None or int(match.group(1)) > 65535:
        raise SystemExit("lifecycle state control address is invalid")
print(run_id, "true" if destroyed else "false")
PY
}
case "$mode" in
  pre-attach)
    [ "$run_id" = - ] || exit 1
    ;;
  attached)
    case "$run_id" in
      [0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f]) ;;
      *) printf '%s\n' "remote cleanup run identity is invalid" >&2; exit 1 ;;
    esac
    [ -x "$run/provisioner" ] && [ ! -L "$run/provisioner" ] || exit 1
    ;;
  *) exit 1 ;;
esac

if lifecycle_value=$(read_lifecycle_state); then
  lifecycle_status=valid
  lifecycle_run_id=${lifecycle_value%% *}
  lifecycle_destroyed=${lifecycle_value#* }
else
  lifecycle_result=$?
  case "$lifecycle_result" in
    2) lifecycle_status=absent ;;
    *) lifecycle_status=invalid ;;
  esac
fi
provisioner_started=0
provisioner_marker_invalid=0
if [ -e "$run/provisioner.started" ] || [ -L "$run/provisioner.started" ]; then
  provisioner_started=1
  if [ ! -f "$run/provisioner.started" ] || [ -L "$run/provisioner.started" ]; then
    provisioner_marker_invalid=1
  fi
fi
if [ -e "$run/provisioner.pid" ] || [ -L "$run/provisioner.pid" ]; then
  provisioner_started=1
fi

case "$lifecycle_status" in
  valid)
    if [ "$mode" = attached ] && [ "$lifecycle_run_id" != "$run_id" ]; then
      printf '%s\n' "remote cleanup run identity does not match lifecycle state" >&2
      exit 1
    fi
    stop_process adapter "$run/adapter.pid" "$run/synchrod-pg"
    if [ "$lifecycle_destroyed" = false ]; then
      "$run/provisioner" lifecycle --state-dir "$run/state" destroy "$lifecycle_run_id"
      lifecycle_value=$(read_lifecycle_state)
      [ "${lifecycle_value%% *}" = "$lifecycle_run_id" ] && [ "${lifecycle_value#* }" = true ] || {
        printf '%s\n' "lifecycle destroy did not persist destroyed state" >&2
        exit 1
      }
    fi
    confirm_process_shutdown provisioner "$run/provisioner.pid" "$run/provisioner"
    rm -rf -- "$run"
    ;;
  absent)
    [ "$mode" = pre-attach ] || { printf '%s\n' "attached cleanup lacks lifecycle state" >&2; exit 1; }
    if [ "$provisioner_started" -ne 0 ]; then
      stop_process adapter "$run/adapter.pid" "$run/synchrod-pg"
      if [ -e "$run/provisioner.pid" ] || [ -L "$run/provisioner.pid" ]; then
        stop_process provisioner "$run/provisioner.pid" "$run/provisioner"
      fi
      if [ "$provisioner_marker_invalid" -ne 0 ]; then
        printf '%s\n' "provisioner start marker is unsafe" >&2
      else
        printf '%s\n' "provisioner started without lifecycle state" >&2
      fi
      exit 1
    fi
    [ ! -e "$run/adapter.pid" ] && [ ! -L "$run/adapter.pid" ] || {
      stop_process adapter "$run/adapter.pid" "$run/synchrod-pg"
      printf '%s\n' "adapter state exists before provisioner lifecycle state" >&2
      exit 1
    }
    rm -rf -- "$run"
    ;;
  invalid)
    stop_process adapter "$run/adapter.pid" "$run/synchrod-pg"
    if [ -e "$run/provisioner.pid" ] || [ -L "$run/provisioner.pid" ]; then
      stop_process provisioner "$run/provisioner.pid" "$run/provisioner"
    fi
    printf '%s\n' "remote lifecycle state is invalid; retained fixture for operator recovery" >&2
    exit 1
    ;;
esac
REMOTE
}

cleanup() {
  command_status=$?
  trap - EXIT HUP INT TERM
  set +e
  cleanup_status=0
  if [ -n "$forward_pid" ] && kill -0 "$forward_pid" >/dev/null 2>&1; then
    if ! kill "$forward_pid"; then
      printf '%s\n' "could not stop SSH tunnel process $forward_pid" >&2
      cleanup_status=1
    else
      wait "$forward_pid" >/dev/null 2>&1
    fi
  fi
  if [ "$remote_created" -ne 0 ]; then
    remote_cleanup
    cleanup_error=$?
    if [ "$cleanup_error" -ne 0 ]; then
      printf '%s\n' "remote fixture cleanup failed with status $cleanup_error; retained $remote_run for operator recovery" >&2
      cleanup_status=$cleanup_error
    fi
  fi
  if ! rm -rf -- "$work_dir"; then
    printf '%s\n' "local fixture cleanup failed: $work_dir" >&2
    [ "$cleanup_status" -ne 0 ] || cleanup_status=1
  fi
  if [ "$command_status" -ne 0 ]; then
    exit "$command_status"
  fi
  exit "$cleanup_status"
}
trap cleanup EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

extension="$release_dir/artifacts/synchro-pg-pg18-ubuntu24.04-linux-x64-$version.tar.gz"
adapter="$release_dir/artifacts/synchrod-pg-linux-x64-$version"
seed="$release_dir/artifacts/synchro-seed-linux-x64-$version"
for payload in "$extension" "$adapter" "$seed"; do
  [ -f "$payload" ] && [ ! -L "$payload" ] || { printf '%s\n' "sealed server payload is missing or unsafe: $payload" >&2; exit 1; }
done
extension_hash=$(shasum -a 256 "$extension" | cut -d ' ' -f 1)
adapter_hash=$(shasum -a 256 "$adapter" | cut -d ' ' -f 1)
seed_hash=$(shasum -a 256 "$seed" | cut -d ' ' -f 1)
provisioner_hash=$(shasum -a 256 "$provisioner" | cut -d ' ' -f 1)

mkdir -p "$work_dir/upload/repo/extensions"
cp "$extension" "$work_dir/upload/extension.tar.gz"
cp "$adapter" "$work_dir/upload/synchrod-pg"
cp "$seed" "$work_dir/upload/synchro-seed"
cp "$provisioner" "$work_dir/upload/provisioner"
cp -R "${GITHUB_WORKSPACE:?GITHUB_WORKSPACE is required}/extensions/testdata" "$work_dir/upload/repo/extensions/testdata"

ssh_command "$ssh_target" sh -s -- "$remote_root" "$remote_run" "$run_name" <<'REMOTE'
set -eu
root=$1
run=$2
name=$3
[ "$run" = "$root/$name" ] || exit 1
[ "$(dirname "$run")" = "$root" ] || exit 1
[ "$(basename "$run")" = "$name" ] || exit 1
mkdir -p "$root"
[ ! -L "$root" ] && [ "$(cd "$root" && pwd -P)" = "$root" ] || exit 1
chmod 700 "$root"
[ ! -e "$run" ] || exit 1
mkdir -m 700 "$run"
REMOTE
remote_created=1
tar -cf - -C "$work_dir/upload" . | ssh_command "$ssh_target" tar -xf - -C "$remote_run"

lifecycle_json=$(python3 - "$key_file" "$known_hosts" "$ssh_target" "$remote_run" <<'PY'
import json, sys
key, known, target, run = sys.argv[1:]
print(json.dumps(["ssh", "-i", key, "-o", "BatchMode=yes", "-o", "IdentitiesOnly=yes", "-o", "StrictHostKeyChecking=yes", "-o", f"UserKnownHostsFile={known}", target, f"{run}/provisioner", "lifecycle", "--state-dir", f"{run}/state"], separators=(",", ":")))
PY
)
lifecycle_base64=$(printf '%s' "$lifecycle_json" | base64 | tr -d '\n')

ssh_command "$ssh_target" sh -s -- "$remote_run" "$pg18_bin_dir" "$extension_hash" "$adapter_hash" "$seed_hash" "$provisioner_hash" "$lifecycle_base64" <<'REMOTE'
set -eu
run=$1
pgbin=$2
extension_hash=$3
adapter_hash=$4
seed_hash=$5
provisioner_hash=$6
lifecycle_json=$(printf '%s' "$7" | base64 -d)
[ ! -L "$run" ] && [ "$(cd "$run" && pwd -P)" = "$run" ] || exit 1
[ "$(uname -s)" = Linux ] && [ "$(uname -m)" = x86_64 ] || exit 1
. /etc/os-release
[ "$ID" = ubuntu ] && [ "$VERSION_ID" = 24.04 ] || exit 1
[ -x "$pgbin/postgres" ] && [ -x "$pgbin/pg_config" ] || exit 1
"$pgbin/postgres" --version | grep -E ' 18[.]3$' >/dev/null
hash() { sha256sum "$1" | cut -d ' ' -f 1; }
[ "$(hash "$run/extension.tar.gz")" = "$extension_hash" ]
[ "$(hash "$run/synchrod-pg")" = "$adapter_hash" ]
[ "$(hash "$run/synchro-seed")" = "$seed_hash" ]
[ "$(hash "$run/provisioner")" = "$provisioner_hash" ]
chmod 700 "$run/synchrod-pg" "$run/synchro-seed" "$run/provisioner"
mkdir -m 700 "$run/extension" "$run/adapter"
tar -xzf "$run/extension.tar.gz" -C "$run/extension"
[ -f "$run/extension/extension/artifact-manifest.json" ]
mv "$run/extension/extension" "$run/extension-bundle"
rmdir "$run/extension"
cp "$run/synchrod-pg" "$run/adapter/synchrod-pg"
printf '%s\n' "$adapter_hash" > "$run/adapter/synchrod-pg.sha256"
chmod 600 "$run/adapter/synchrod-pg.sha256"
http_port=$(python3 - <<'PY'
import socket
s = socket.socket()
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()
PY
)
printf '%s\n' "$http_port" > "$run/http.port"
: > "$run/provisioner.started"
chmod 600 "$run/provisioner.started"
nohup "$run/provisioner" start \
  --pg18-bin-dir "$pgbin" \
  --extension-artifact "$run/extension-bundle" \
  --adapter-artifact "$run/adapter/synchrod-pg" \
  --state-dir "$run/state" \
  --temp-parent "$run" \
  --url-file "$run/admin.url" \
  --attach-environment-file "$run/attach.env" \
  --lifecycle-command-json "$lifecycle_json" \
  >"$run/provisioner.log" 2>&1 &
provisioner_pid=$!
printf '%s\n' "$provisioner_pid" > "$run/provisioner.pid"
REMOTE

ssh_command "$ssh_target" sh -s -- "$remote_run" <<'REMOTE'
set -eu
run=$1
[ ! -L "$run" ] && [ "$(cd "$run" && pwd -P)" = "$run" ] || exit 1
[ -f "$run/provisioner.pid" ] && [ ! -L "$run/provisioner.pid" ] || { printf '%s\n' "provisioner pid file is missing or unsafe" >&2; exit 1; }
provisioner_pid=$(cat "$run/provisioner.pid")
case "$provisioner_pid" in *[!0-9]*|'') printf '%s\n' "provisioner pid is invalid" >&2; exit 1 ;; esac
provisioner_diagnostics() {
  if [ -f "$run/provisioner.log" ] && [ ! -L "$run/provisioner.log" ]; then
    tail -c 16384 -- "$run/provisioner.log" >&2
  else
    printf '%s\n' "provisioner log is missing or unsafe" >&2
  fi
}
for unused in $(seq 1 3600); do
  if ! kill -0 "$provisioner_pid" >/dev/null 2>&1; then
    printf '%s\n' "provisioner exited before attach environment became available" >&2
    provisioner_diagnostics
    exit 1
  fi
  if [ ! -r "/proc/$provisioner_pid/cmdline" ]; then
    printf '%s\n' "provisioner process identity is unreadable" >&2
    provisioner_diagnostics
    exit 1
  fi
  if ! tr '\000' '\n' < "/proc/$provisioner_pid/cmdline" | grep -Fqx -- "$run/provisioner"; then
    printf '%s\n' "provisioner process identity does not match the owned executable" >&2
    provisioner_diagnostics
    exit 1
  fi
  [ -f "$run/attach.env" ] && exit 0
  sleep 1
done
printf '%s\n' "provisioner attach environment was unavailable after 3600 seconds" >&2
provisioner_diagnostics
exit 1
REMOTE

ssh_command "$ssh_target" sh -s -- "$remote_run" <<'REMOTE'
set -eu
run=$1
[ ! -L "$run" ] && [ "$(cd "$run" && pwd -P)" = "$run" ] || exit 1
[ -f "$run/attach.env" ] && [ ! -L "$run/attach.env" ] || exit 1
http_port=$(cat "$run/http.port")
case "$http_port" in *[!0-9]*|'') printf '%s\n' "remote HTTP port is invalid" >&2; exit 1 ;; esac
[ "$http_port" -ge 1 ] && [ "$http_port" -le 65535 ] || { printf '%s\n' "remote HTTP port is invalid" >&2; exit 1; }
SYNCHRO_ATTACH_DIR="$run/state"
export SYNCHRO_ATTACH_DIR
. "$run/attach.env"
admin_password=$(cat "$SYNCHRO_CONFORMANCE_ADMIN_PASSWORD_FILE")
adapter_password=$(cat "$SYNCHRO_CONFORMANCE_ADAPTER_PASSWORD_FILE")
admin_url=$(python3 - "$SYNCHRO_CONFORMANCE_ATTACH_DATABASE_URL" "$SYNCHRO_CONFORMANCE_ADMIN_USER" "$admin_password" <<'PY'
import sys, urllib.parse
base, user, password = sys.argv[1:]
value = urllib.parse.urlsplit(base)
netloc = urllib.parse.quote(user, safe="") + ":" + urllib.parse.quote(password, safe="") + "@" + value.netloc
print(urllib.parse.urlunsplit((value.scheme, netloc, value.path, value.query, "")))
PY
)
adapter_url=$(python3 - "$SYNCHRO_CONFORMANCE_ATTACH_DATABASE_URL" "$SYNCHRO_CONFORMANCE_ADAPTER_USER" "$adapter_password" <<'PY'
import sys, urllib.parse
base, user, password = sys.argv[1:]
value = urllib.parse.urlsplit(base)
netloc = urllib.parse.quote(user, safe="") + ":" + urllib.parse.quote(password, safe="") + "@" + value.netloc
print(urllib.parse.urlunsplit((value.scheme, netloc, value.path, value.query, "")))
PY
)
unset admin_password adapter_password
DATABASE_URL="$admin_url" "$run/provisioner" prepare --repo-root "$run/repo" --database-url "$admin_url"
DATABASE_URL="$adapter_url" JWT_SECRET=$(cat "$SYNCHRO_CONFORMANCE_JWT_SECRET_FILE") LISTEN_ADDR="127.0.0.1:$http_port" \
  nohup "$run/synchrod-pg" >"$run/adapter.log" 2>&1 &
printf '%s\n' "$!" > "$run/adapter.pid"
for unused in $(seq 1 60); do curl --fail --silent "http://127.0.0.1:$http_port/ready" >/dev/null && exit 0; sleep 1; done
cat "$run/adapter.log" >&2
exit 1
REMOTE

mkdir -m 700 "$work_dir/attach"
scp_command "$ssh_target:$remote_run/attach.env" "$work_dir/attach/attach.remote.env" >/dev/null
ssh_command "$ssh_target" tar -cf - -C "$remote_run/state" admin-password adapter-password observer-password worker-password operator-password jwt-secret \
  | tar -xf - -C "$work_dir/attach"
chmod 600 "$work_dir/attach"/*
SYNCHRO_ATTACH_DIR="$work_dir/attach"
export SYNCHRO_ATTACH_DIR
# shellcheck disable=SC1091
. "$work_dir/attach/attach.remote.env"
run_id=$SYNCHRO_CONFORMANCE_ATTACH_RUN_ID
case "$run_id" in [0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f]) ;; *) printf '%s\n' "remote lifecycle run identity is invalid" >&2; exit 1 ;; esac
remote_pg_port=$(python3 - "$SYNCHRO_CONFORMANCE_ATTACH_DATABASE_URL" <<'PY'
import sys, urllib.parse
value = urllib.parse.urlsplit(sys.argv[1])
if value.hostname != "127.0.0.1" or value.port is None:
    raise SystemExit("remote attach URL is not loopback")
print(value.port)
PY
)
remote_http_port=$(ssh_command "$ssh_target" cat "$remote_run/http.port")
# Lifecycle restart responses keep the remote loopback port.
# Only PostgreSQL must preserve that port through the SSH tunnel.
local_pg_port=$remote_pg_port
local_http_port=$(select_free_port "$local_pg_port")
SYNCHROD_PG_PORT=$(select_free_port "$local_pg_port" "$local_http_port")
export SYNCHROD_PG_PORT
ssh_command -N \
  -o ExitOnForwardFailure=yes \
  -o ServerAliveInterval=15 \
  -L "127.0.0.1:$local_pg_port:127.0.0.1:$remote_pg_port" \
  -L "127.0.0.1:$local_http_port:127.0.0.1:$remote_http_port" \
  "$ssh_target" &
forward_pid=$!
sleep 2
kill -0 "$forward_pid"
for _ in $(seq 1 30); do curl --fail --silent "http://127.0.0.1:$local_http_port/ready" >/dev/null && break; sleep 1; done
curl --fail --silent "http://127.0.0.1:$local_http_port/ready" >/dev/null

local_database_url=$(python3 - "$SYNCHRO_CONFORMANCE_ATTACH_DATABASE_URL" "$local_pg_port" <<'PY'
import sys, urllib.parse
value = urllib.parse.urlsplit(sys.argv[1])
print(urllib.parse.urlunsplit((value.scheme, "127.0.0.1:" + sys.argv[2], value.path, value.query, "")))
PY
)
cp "$work_dir/attach/attach.remote.env" "$work_dir/attach/attach.env"
printf "SYNCHRO_CONFORMANCE_ATTACH_DATABASE_URL='%s'\n" "$local_database_url" >> "$work_dir/attach/attach.env"
set -a
# shellcheck disable=SC1091
. "$work_dir/attach/attach.env"
set +a
export SYNCHRO_TEST_URL="http://127.0.0.1:$local_http_port"
export SYNCHRO_CONFORMANCE_JWT_SECRET_FILE="$work_dir/attach/jwt-secret"
SYNCHRO_TEST_JWT_SECRET=$(cat "$SYNCHRO_CONFORMANCE_JWT_SECRET_FILE")
export SYNCHRO_TEST_JWT_SECRET
admin_password=$(cat "$SYNCHRO_CONFORMANCE_ADMIN_PASSWORD_FILE")
ADAPTER_TEST_URL=$(python3 - "$local_database_url" "$SYNCHRO_CONFORMANCE_ADMIN_USER" "$admin_password" <<'PY'
import sys, urllib.parse
value = urllib.parse.urlsplit(sys.argv[1])
auth = urllib.parse.quote(sys.argv[2], safe="") + ":" + urllib.parse.quote(sys.argv[3], safe="")
print(urllib.parse.urlunsplit((value.scheme, auth + "@" + value.netloc, value.path, value.query, "")))
PY
)
unset admin_password
REPLICATION_URL=$ADAPTER_TEST_URL
WARM_CONNECT_ENV_FILE="$work_dir/attach/attach.env"
export ADAPTER_TEST_URL REPLICATION_URL WARM_CONNECT_ENV_FILE
"$@"
