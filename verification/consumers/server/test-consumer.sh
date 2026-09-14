#!/bin/sh
set -eu

if [ "$#" -ne 10 ]; then
  echo "usage: $0 <pg18-bindir> <extension-archive> <provisioner> <adapter> <seed> <listen-url> <repo-root> <cell> <result> <sealed-hashes>" >&2
  exit 2
fi

pg18_bindir=$1
extension_archive=$2
provisioner=$3
adapter=$4
seed=$5
listen_url=$6
repo_root=$7
cell_id=$8
cell_result=$9
sealed_hashes=${10}
tool=$repo_root/verification/packaged_smoke.py

case "$(uname -s):$(uname -m)" in Linux:x86_64) ;; *) echo "Linux x64 is required" >&2; exit 1 ;; esac
case "$listen_url" in http://127.0.0.1:[0-9]*) ;; *) echo "loopback HTTP listen URL is required" >&2; exit 1 ;; esac
test -d "$pg18_bindir"
test -f "$extension_archive"
test -x "$provisioner"
test -x "$adapter"
test -x "$seed"

work_dir=$(mktemp -d "${TMPDIR:-/tmp}/synchro-server-consumer.XXXXXX")
provisioner_pid=
adapter_pid=
destroyed=0
attach_run_id=
cleanup() {
  test -z "$adapter_pid" || kill "$adapter_pid" >/dev/null 2>&1 || true
  if [ "$destroyed" -eq 0 ] && [ -n "$provisioner_pid" ]; then
    if [ -n "$attach_run_id" ]; then
      "$provisioner" lifecycle --state-dir "$work_dir/state" destroy "$attach_run_id" >/dev/null 2>&1 || true
    else
      kill "$provisioner_pid" >/dev/null 2>&1 || true
    fi
  fi
  test -z "$provisioner_pid" || wait "$provisioner_pid" 2>/dev/null || true
  rm -rf "$work_dir"
}
trap cleanup EXIT HUP INT TERM

mkdir -p "$work_dir/unpacked" "$work_dir/adapter"
tar -xf "$extension_archive" -C "$work_dir/unpacked"
if [ -f "$work_dir/unpacked/extension/artifact-manifest.json" ]; then
  extension_dir=$work_dir/unpacked/extension
elif [ -f "$work_dir/unpacked/artifact-manifest.json" ]; then
  extension_dir=$work_dir/unpacked
else
  echo "extension archive has no artifact manifest" >&2
  exit 1
fi
adapter_hash=$(sha256sum "$adapter" | awk '{print $1}')
cp "$adapter" "$work_dir/adapter/synchrod-pg"
printf '%s\n' "$adapter_hash" > "$work_dir/adapter/synchrod-pg.sha256"

"$provisioner" start --pg18-bin-dir "$pg18_bindir" --extension-artifact "$extension_dir" --adapter-artifact "$work_dir/adapter/synchrod-pg" --state-dir "$work_dir/state" --temp-parent "$work_dir" --url-file "$work_dir/admin.url" --attach-environment-file "$work_dir/attach.env" >"$work_dir/provisioner.log" 2>&1 &
provisioner_pid=$!
for _ in $(seq 1 90); do test -f "$work_dir/attach.env" && break; sleep 1; done
test -f "$work_dir/attach.env"
SYNCHRO_ATTACH_DIR=$work_dir/state
export SYNCHRO_ATTACH_DIR
. "$work_dir/attach.env"
attach_run_id=$SYNCHRO_CONFORMANCE_ATTACH_RUN_ID

admin_password=$(cat "$SYNCHRO_CONFORMANCE_ADMIN_PASSWORD_FILE")
adapter_password=$(cat "$SYNCHRO_CONFORMANCE_ADAPTER_PASSWORD_FILE")
make_url() { python3 -c 'import sys, urllib.parse; base, user, credential = sys.argv[1:]; value=urllib.parse.urlsplit(base); print(urllib.parse.urlunsplit((value.scheme, urllib.parse.quote(user,safe="")+":"+urllib.parse.quote(credential,safe="")+"@"+value.netloc, value.path, value.query, "")))' "$SYNCHRO_CONFORMANCE_ATTACH_DATABASE_URL" "$1" "$2"; }
admin_url=$(make_url "$SYNCHRO_CONFORMANCE_ADMIN_USER" "$admin_password")
adapter_url=$(make_url "$SYNCHRO_CONFORMANCE_ADAPTER_USER" "$adapter_password")
unset admin_password adapter_password
DATABASE_URL="$admin_url" "$provisioner" prepare --repo-root "$repo_root" --database-url "$admin_url"

start_adapter() {
  DATABASE_URL="$adapter_url" JWT_SECRET="$(cat "$SYNCHRO_CONFORMANCE_JWT_SECRET_FILE")" LISTEN_ADDR="${listen_url#http://}" "$adapter" >"$work_dir/adapter.log" 2>&1 &
  adapter_pid=$!
  for _ in $(seq 1 60); do curl --fail --silent "$listen_url/ready" >/dev/null && return; sleep 1; done
  cat "$work_dir/adapter.log" >&2
  return 1
}
start_adapter
mkdir -p "$work_dir/protocol-state"
go run "$repo_root/verification/consumers/server/public_smoke.go" --url "$listen_url" --jwt-secret-file "$SYNCHRO_CONFORMANCE_JWT_SECRET_FILE" --phase initial --adapter-pid "$adapter_pid" --state-dir "$work_dir/protocol-state" --output "$work_dir/initial.json"
killed_pid=$adapter_pid
kill -9 "$adapter_pid"; wait "$adapter_pid" 2>/dev/null || true; adapter_pid=
start_adapter
go run "$repo_root/verification/consumers/server/public_smoke.go" --url "$listen_url" --jwt-secret-file "$SYNCHRO_CONFORMANCE_JWT_SECRET_FILE" --phase resume --adapter-pid "$adapter_pid" --state-dir "$work_dir/protocol-state" --output "$work_dir/resume.json"
DATABASE_URL="$admin_url" "$seed" --output "$work_dir/seed.sqlite"
test -s "$work_dir/seed.sqlite"
set -- python3 "$tool" complete-server-cell --repo-root "$repo_root" --cell "$cell_id" --output "$cell_result" --initial "$work_dir/initial.json" --resume "$work_dir/resume.json" --killed-pid "$killed_pid" --artifact "$extension_archive" --artifact "$adapter" --artifact "$seed"
for hash in $sealed_hashes; do set -- "$@" --expected-artifact-hash "$hash"; done
"$@"
"$provisioner" lifecycle --state-dir "$work_dir/state" destroy "$attach_run_id" >/dev/null
destroyed=1
wait "$provisioner_pid"
provisioner_pid=
