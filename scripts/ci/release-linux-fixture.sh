#!/bin/sh
# Run one Apple package cell against an isolated Linux release fixture.
set -eu

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
[ -x "$provisioner" ] && [ ! -L "$provisioner" ] || { printf '%s\n' "provisioner is missing or unsafe" >&2; exit 1; }
case "$version" in [0-9]*.[0-9]*.[0-9]*) ;; *) printf '%s\n' "release version is invalid" >&2; exit 1 ;; esac
case "$ssh_user" in ''|*[!A-Za-z0-9_.-]*) printf '%s\n' "SSH user is invalid" >&2; exit 1 ;; esac
case "$ssh_host" in ''|*[!A-Za-z0-9:._-]*) printf '%s\n' "SSH host is invalid" >&2; exit 1 ;; esac
case "$remote_root" in /*) ;; *) printf '%s\n' "remote root must be absolute" >&2; exit 1 ;; esac
case "$remote_root" in *[!A-Za-z0-9_./-]*|*/../*|*/..|*//*|/) printf '%s\n' "remote root is unsafe" >&2; exit 1 ;; esac
case "$pg18_bin_dir" in /*) ;; *) printf '%s\n' "PostgreSQL bindir must be absolute" >&2; exit 1 ;; esac
case "$pg18_bin_dir" in *[!A-Za-z0-9_./-]*|*/../*|*/..|*//*) printf '%s\n' "PostgreSQL bindir is unsafe" >&2; exit 1 ;; esac
case "$cell" in SUP-IOS-MIN-001|SUP-IOS-CURRENT-001|SUP-RN-IOS-CURRENT-001) ;; *) printf '%s\n' "fixture cell is invalid" >&2; exit 1 ;; esac

: "${GITHUB_RUN_ID:?GITHUB_RUN_ID is required}"
: "${GITHUB_RUN_ATTEMPT:?GITHUB_RUN_ATTEMPT is required}"
: "${GITHUB_JOB:?GITHUB_JOB is required}"
case "$GITHUB_RUN_ID:$GITHUB_RUN_ATTEMPT:$GITHUB_JOB" in *[!A-Za-z0-9_.:-]*) printf '%s\n' "GitHub run identity is invalid" >&2; exit 1 ;; esac
run_name="run-${GITHUB_RUN_ID}-${GITHUB_RUN_ATTEMPT}-${GITHUB_JOB}-${cell}"
remote_run="$remote_root/$run_name"

ssh_target="$ssh_user@$ssh_host"
ssh_command() {
  ssh -i "$key_file" -o BatchMode=yes -o IdentitiesOnly=yes -o StrictHostKeyChecking=yes -o "UserKnownHostsFile=$known_hosts" "$@"
}
scp_command() {
  scp -i "$key_file" -o BatchMode=yes -o IdentitiesOnly=yes -o StrictHostKeyChecking=yes -o "UserKnownHostsFile=$known_hosts" "$@"
}
work_dir=$(mktemp -d "${RUNNER_TEMP:-${TMPDIR:-/tmp}}/synchro-linux-fixture.XXXXXX")
forward_pid=
remote_created=0
run_id=

remote_cleanup() {
  ssh_command "$ssh_target" sh -s -- "$remote_root" "$remote_run" "$run_name" "$run_id" <<'REMOTE' >/dev/null 2>&1 || true
set -eu
root=$1
run=$2
name=$3
run_id=$4
[ "$run" = "$root/$name" ] || exit 1
[ "$(dirname "$run")" = "$root" ] || exit 1
[ "$(basename "$run")" = "$name" ] || exit 1
[ ! -L "$root" ] && [ ! -L "$run" ] || exit 1
if [ -n "$run_id" ] && [ -x "$run/provisioner" ]; then
  "$run/provisioner" lifecycle --state-dir "$run/state" destroy "$run_id" >/dev/null 2>&1 || true
elif [ -f "$run/provisioner.pid" ]; then
  pid=$(cat "$run/provisioner.pid")
  case "$pid" in *[!0-9]*|'') ;; *) kill "$pid" >/dev/null 2>&1 || true ;; esac
fi
if [ -f "$run/adapter.pid" ]; then
  pid=$(cat "$run/adapter.pid")
  case "$pid" in *[!0-9]*|'') ;; *) kill "$pid" >/dev/null 2>&1 || true ;; esac
fi
rm -rf -- "$run"
REMOTE
}

cleanup() {
  [ -z "$forward_pid" ] || kill "$forward_pid" >/dev/null 2>&1 || true
  [ "$remote_created" -eq 0 ] || remote_cleanup
  rm -rf "$work_dir"
}
trap cleanup EXIT HUP INT TERM

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
printf '%s\n' "$!" > "$run/provisioner.pid"
for unused in $(seq 1 120); do [ -f "$run/attach.env" ] && break; sleep 1; done
[ -f "$run/attach.env" ]
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
# Matching local ports keep those responses valid through the SSH tunnel.
local_pg_port=$remote_pg_port
local_http_port=$remote_http_port
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
"$@"
