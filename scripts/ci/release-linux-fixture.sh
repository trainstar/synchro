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
launch_dispatched=0
run_id=

ssh_target="$ssh_user@$ssh_host"
ssh_command() {
  ssh -i "$key_file" -o BatchMode=yes -o IdentitiesOnly=yes -o StrictHostKeyChecking=yes -o "UserKnownHostsFile=$known_hosts" "$@"
}
scp_command() {
  scp -i "$key_file" -o BatchMode=yes -o IdentitiesOnly=yes -o StrictHostKeyChecking=yes -o "UserKnownHostsFile=$known_hosts" "$@"
}

pidfd_helper_base64=$(base64 <<'PY' | tr -d '\n'
import os
import re
import select
import signal
import stat
import sys
import time

RECORD = re.compile(rb"([1-9][0-9]*) ([1-9][0-9]*)\n?")
DIAGNOSTIC_LIMIT = 16384


class IdentityError(Exception):
    pass


def safe_regular(path: str, maximum: int) -> os.stat_result:
    try:
        info = os.lstat(path)
    except FileNotFoundError as error:
        raise error
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode) or info.st_mode & 0o077:
        raise IdentityError(f"unsafe file: {path}")
    if info.st_size > maximum:
        raise IdentityError(f"oversized file: {path}")
    return info


def read_start_time(pid: int) -> int:
    try:
        with open(f"/proc/{pid}/stat", "rb") as stream:
            data = stream.read(4097)
    except FileNotFoundError as error:
        raise ProcessLookupError(pid) from error
    except OSError as error:
        raise IdentityError(f"process stat is unreadable: {error}") from error
    if len(data) > 4096:
        raise IdentityError("process stat is oversized")
    end = data.rfind(b")")
    fields = data[end + 2 :].split() if end >= 0 else []
    if len(fields) <= 19 or not fields[19].isdigit():
        raise IdentityError("process start time is invalid")
    return int(fields[19])


def read_executable(pid: int) -> str:
    try:
        return os.readlink(f"/proc/{pid}/exe")
    except FileNotFoundError as error:
        raise ProcessLookupError(pid) from error
    except OSError as error:
        raise IdentityError(f"process executable is unreadable: {error}") from error


def open_pidfd(pid: int) -> int:
    try:
        return os.pidfd_open(pid, 0)
    except ProcessLookupError:
        raise
    except (AttributeError, OSError) as error:
        raise IdentityError(f"pidfd is unavailable: {error}") from error


def validate_owned_process(pid: int, start_time: int, expected: str) -> int | None:
    try:
        descriptor = open_pidfd(pid)
    except ProcessLookupError:
        return None
    try:
        try:
            actual_start = read_start_time(pid)
            actual_executable = read_executable(pid)
        except ProcessLookupError:
            os.close(descriptor)
            return None
        if actual_start != start_time:
            raise IdentityError("process start time does not match the owned process")
        if actual_executable != expected:
            raise IdentityError("process executable does not match the owned process")
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def read_record(path: str, required: bool) -> tuple[int, int] | None:
    try:
        safe_regular(path, 128)
    except FileNotFoundError:
        if required:
            raise IdentityError(f"process identity is missing: {path}")
        return None
    try:
        with open(path, "rb") as stream:
            data = stream.read(129)
    except OSError as error:
        raise IdentityError(f"process identity is unreadable: {error}") from error
    match = RECORD.fullmatch(data)
    if match is None:
        raise IdentityError("process identity record is invalid")
    return int(match.group(1)), int(match.group(2))


def write_record(pid: int, path: str, expected: str) -> None:
    try:
        descriptor = open_pidfd(pid)
    except ProcessLookupError as error:
        raise IdentityError("process exited before its identity was recorded") from error
    try:
        start_time = read_start_time(pid)
        poller = select.poll()
        poller.register(descriptor, select.POLLIN)
        deadline = time.monotonic() + 5
        while True:
            try:
                actual_executable = read_executable(pid)
            except ProcessLookupError as error:
                raise IdentityError("process exited before its identity was recorded") from error
            if read_start_time(pid) != start_time:
                raise IdentityError("process identity changed before it was recorded")
            if actual_executable == expected:
                break
            if time.monotonic() >= deadline or poller.poll(10):
                raise IdentityError("process executable did not become the owned executable")
            time.sleep(0.01)
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        output = os.open(path, flags, 0o600)
        try:
            os.write(output, f"{pid} {start_time}\n".encode("ascii"))
            os.fsync(output)
        finally:
            os.close(output)
    finally:
        os.close(descriptor)


def diagnostics(path: str) -> None:
    try:
        info = safe_regular(path, 64 << 20)
        with open(path, "rb") as stream:
            stream.seek(max(0, info.st_size - DIAGNOSTIC_LIMIT))
            data = stream.read(DIAGNOSTIC_LIMIT)
    except (FileNotFoundError, IdentityError, OSError):
        print("provisioner log is missing or unsafe", file=sys.stderr)
        return
    sys.stderr.buffer.write(data)
    if data and not data.endswith(b"\n"):
        sys.stderr.buffer.write(b"\n")


def wait_for_attach(record: str, expected: str, attach: str, log: str, timeout: int) -> None:
    identity = read_record(record, True)
    assert identity is not None
    descriptor = validate_owned_process(identity[0], identity[1], expected)
    if descriptor is None:
        print("provisioner exited before attach environment became available", file=sys.stderr)
        diagnostics(log)
        raise SystemExit(1)
    poller = select.poll()
    poller.register(descriptor, select.POLLIN)
    deadline = time.monotonic() + timeout
    try:
        while True:
            try:
                info = os.lstat(attach)
            except FileNotFoundError:
                pass
            else:
                if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
                    raise IdentityError("attach environment is unsafe")
                return
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                print(f"provisioner attach environment was unavailable after {timeout} seconds", file=sys.stderr)
                diagnostics(log)
                raise SystemExit(1)
            if poller.poll(min(1000, max(1, int(remaining * 1000)))):
                print("provisioner exited before attach environment became available", file=sys.stderr)
                diagnostics(log)
                raise SystemExit(1)
    finally:
        os.close(descriptor)


def stop_owned(record: str, expected: str, timeout: int, required: bool) -> None:
    identity = read_record(record, required)
    if identity is None:
        return
    descriptor = validate_owned_process(identity[0], identity[1], expected)
    if descriptor is None:
        return
    try:
        try:
            signal.pidfd_send_signal(descriptor, signal.SIGTERM, None, 0)
        except ProcessLookupError:
            return
        except (AttributeError, OSError) as error:
            raise IdentityError(f"pidfd signal failed: {error}") from error
        poller = select.poll()
        poller.register(descriptor, select.POLLIN)
        if not poller.poll(timeout * 1000):
            raise IdentityError("owned process did not stop")
    finally:
        os.close(descriptor)


def main() -> None:
    operation = sys.argv[1]
    if operation == "record" and len(sys.argv) == 5:
        write_record(int(sys.argv[2]), sys.argv[3], sys.argv[4])
    elif operation == "wait" and len(sys.argv) == 7:
        wait_for_attach(sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5], int(sys.argv[6]))
    elif operation == "stop" and len(sys.argv) == 6:
        stop_owned(sys.argv[2], sys.argv[3], int(sys.argv[4]), sys.argv[5] == "required")
    else:
        raise IdentityError("pidfd helper arguments are invalid")


try:
    main()
except (IdentityError, ProcessLookupError, ValueError) as error:
    print(f"pidfd helper: {error}", file=sys.stderr)
    raise SystemExit(1)
PY
)

remote_cleanup() {
  if [ -n "$run_id" ]; then
    cleanup_mode=attached
    cleanup_run_id=$run_id
  elif [ "$launch_dispatched" -ne 0 ]; then
    cleanup_mode=pre-attach
    cleanup_run_id=-
  else
    cleanup_mode=pre-launch
    cleanup_run_id=-
  fi
  ssh_command "$ssh_target" sh -s -- "$remote_root" "$remote_run" "$run_name" "$cleanup_mode" "$cleanup_run_id" "$pidfd_helper_base64" <<'REMOTE'
set -eu
root=$1
run=$2
name=$3
mode=$4
run_id=$5
pidfd_helper=$6
[ "$run" = "$root/$name" ] || exit 1
[ "$(dirname "$run")" = "$root" ] || exit 1
[ "$(basename "$run")" = "$name" ] || exit 1
[ -d "$root" ] && [ ! -L "$root" ] && [ "$(cd "$root" && pwd -P)" = "$root" ] || exit 1
[ ! -e "$run" ] && exit 0
[ -d "$run" ] && [ ! -L "$run" ] && [ "$(cd "$run" && pwd -P)" = "$run" ] || exit 1
pidfd() {
  printf '%s' "$pidfd_helper" | base64 -d | python3 - "$@"
}
read_lifecycle_run_id() {
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

class ObjectPairs(list):
    pass

try:
    with open(path, "r", encoding="utf-8") as stream:
        value = json.load(stream, object_pairs_hook=ObjectPairs)
except (OSError, UnicodeError, ValueError, json.JSONDecodeError) as error:
    raise SystemExit(f"lifecycle state is invalid: {error}")
if not isinstance(value, ObjectPairs):
    raise SystemExit("lifecycle state is not an object")
run_ids = [item for key, item in value if key == "run_id"]
if len(run_ids) != 1:
    raise SystemExit("lifecycle state must contain one run ID")
run_id = run_ids[0]
if not isinstance(run_id, str) or re.fullmatch(r"[0-9a-f]{32}", run_id) is None:
    raise SystemExit("lifecycle state run ID is invalid")
print(run_id)
PY
}
case "$mode" in
  pre-launch)
    [ "$run_id" = - ] || exit 1
    rm -rf -- "$run"
    exit 0
    ;;
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

if lifecycle_run_id=$(read_lifecycle_run_id); then
  lifecycle_status=usable
else
  lifecycle_result=$?
  case "$lifecycle_result" in
    2) lifecycle_status=absent ;;
    *) lifecycle_status=unusable ;;
  esac
fi

case "$lifecycle_status" in
  usable)
    if [ "$mode" = attached ] && [ "$lifecycle_run_id" != "$run_id" ]; then
      printf '%s\n' "remote cleanup run identity does not match lifecycle state" >&2
      exit 1
    fi
    [ -x "$run/provisioner" ] && [ ! -L "$run/provisioner" ] || exit 1
    "$run/provisioner" lifecycle --state-dir "$run/state" destroy "$lifecycle_run_id"
    pidfd stop "$run/adapter.pid" "$run/synchrod-pg" 30 optional
    rm -rf -- "$run"
    ;;
  absent|unusable)
    [ "$mode" = pre-attach ] || { printf '%s\n' "attached cleanup lacks usable lifecycle state" >&2; exit 1; }
    pidfd stop "$run/provisioner.pid" "$run/provisioner" 30 required
    printf '%s\n' "pre-attach cleanup lacks usable lifecycle state" >&2
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

launch_dispatched=1
ssh_command "$ssh_target" sh -s -- "$remote_run" "$pg18_bin_dir" "$extension_hash" "$adapter_hash" "$seed_hash" "$provisioner_hash" "$lifecycle_base64" "$pidfd_helper_base64" <<'REMOTE'
set -eu
run=$1
pgbin=$2
extension_hash=$3
adapter_hash=$4
seed_hash=$5
provisioner_hash=$6
lifecycle_json=$(printf '%s' "$7" | base64 -d)
pidfd_helper=$8
pidfd() {
  printf '%s' "$pidfd_helper" | base64 -d | python3 - "$@"
}
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
pidfd record "$provisioner_pid" "$run/provisioner.pid" "$run/provisioner"
REMOTE

ssh_command "$ssh_target" sh -s -- "$remote_run" "$pidfd_helper_base64" <<'REMOTE'
set -eu
run=$1
pidfd_helper=$2
[ ! -L "$run" ] && [ "$(cd "$run" && pwd -P)" = "$run" ] || exit 1
pidfd() {
  printf '%s' "$pidfd_helper" | base64 -d | python3 - "$@"
}
pidfd wait "$run/provisioner.pid" "$run/provisioner" "$run/attach.env" "$run/provisioner.log" 3600
REMOTE

ssh_command "$ssh_target" sh -s -- "$remote_run" "$pidfd_helper_base64" <<'REMOTE'
set -eu
run=$1
pidfd_helper=$2
pidfd() {
  printf '%s' "$pidfd_helper" | base64 -d | python3 - "$@"
}
[ ! -L "$run" ] && [ "$(cd "$run" && pwd -P)" = "$run" ] || exit 1
[ -f "$run/attach.env" ] && [ ! -L "$run/attach.env" ] || exit 1
http_port=$(cat "$run/http.port")
case "$http_port" in *[!0-9]*|'') printf '%s\n' "remote HTTP port is invalid" >&2; exit 1 ;; esac
[ "$http_port" -ge 1 ] && [ "$http_port" -le 65535 ] || { printf '%s\n' "remote HTTP port is invalid" >&2; exit 1; }
SYNCHRO_ATTACH_DIR="$run/state"
export SYNCHRO_ATTACH_DIR
. "$run/attach.env"
admin_url=$(python3 - "$SYNCHRO_CONFORMANCE_ATTACH_DATABASE_URL" "$SYNCHRO_CONFORMANCE_ADMIN_USER" "$SYNCHRO_CONFORMANCE_ADMIN_PASSWORD_FILE" <<'PY'
import sys, urllib.parse
base, user, password_file = sys.argv[1:]
with open(password_file, encoding="utf-8") as stream:
    password = stream.read().rstrip("\n")
value = urllib.parse.urlsplit(base)
netloc = urllib.parse.quote(user, safe="") + ":" + urllib.parse.quote(password, safe="") + "@" + value.netloc
print(urllib.parse.urlunsplit((value.scheme, netloc, value.path, value.query, "")))
PY
)
adapter_url=$(python3 - "$SYNCHRO_CONFORMANCE_ATTACH_DATABASE_URL" "$SYNCHRO_CONFORMANCE_ADAPTER_USER" "$SYNCHRO_CONFORMANCE_ADAPTER_PASSWORD_FILE" <<'PY'
import sys, urllib.parse
base, user, password_file = sys.argv[1:]
with open(password_file, encoding="utf-8") as stream:
    password = stream.read().rstrip("\n")
value = urllib.parse.urlsplit(base)
netloc = urllib.parse.quote(user, safe="") + ":" + urllib.parse.quote(password, safe="") + "@" + value.netloc
print(urllib.parse.urlunsplit((value.scheme, netloc, value.path, value.query, "")))
PY
)
DATABASE_URL="$admin_url" "$run/provisioner" prepare --repo-root "$run/repo"
DATABASE_URL="$adapter_url" JWT_SECRET=$(cat "$SYNCHRO_CONFORMANCE_JWT_SECRET_FILE") LISTEN_ADDR="127.0.0.1:$http_port" \
  nohup "$run/synchrod-pg" >"$run/adapter.log" 2>&1 &
adapter_pid=$!
pidfd record "$adapter_pid" "$run/adapter.pid" "$run/synchrod-pg"
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
ADAPTER_TEST_URL=$(python3 - "$local_database_url" "$SYNCHRO_CONFORMANCE_ADMIN_USER" "$SYNCHRO_CONFORMANCE_ADMIN_PASSWORD_FILE" <<'PY'
import sys, urllib.parse
with open(sys.argv[3], encoding="utf-8") as stream:
    password = stream.read().rstrip("\n")
value = urllib.parse.urlsplit(sys.argv[1])
auth = urllib.parse.quote(sys.argv[2], safe="") + ":" + urllib.parse.quote(password, safe="")
print(urllib.parse.urlunsplit((value.scheme, auth + "@" + value.netloc, value.path, value.query, "")))
PY
)
REPLICATION_URL=$ADAPTER_TEST_URL
WARM_CONNECT_ENV_FILE="$work_dir/attach/attach.env"
export ADAPTER_TEST_URL REPLICATION_URL WARM_CONNECT_ENV_FILE
"$@"
