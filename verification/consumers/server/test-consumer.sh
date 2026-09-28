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
if [ ! -f "$work_dir/attach.env" ]; then
  # The work directory is removed on exit, so the provisioner reason must be printed here.
  cat "$work_dir/provisioner.log" >&2 || true
  echo "PostgreSQL provisioner did not become ready" >&2
  exit 1
fi
SYNCHRO_ATTACH_DIR=$work_dir/state
export SYNCHRO_ATTACH_DIR
# shellcheck disable=SC1091
. "$work_dir/attach.env"
attach_run_id=$SYNCHRO_CONFORMANCE_ATTACH_RUN_ID

make_url() {
  python3 -c 'import pathlib, sys, urllib.parse; base, user, path = sys.argv[1:]; credential = pathlib.Path(path).read_text().rstrip("\n"); value=urllib.parse.urlsplit(base); print(urllib.parse.urlunsplit((value.scheme, urllib.parse.quote(user,safe="")+":"+urllib.parse.quote(credential,safe="")+"@"+value.netloc, value.path, value.query, "")))' "$SYNCHRO_CONFORMANCE_ATTACH_DATABASE_URL" "$1" "$2"
}
admin_url=$(make_url "$SYNCHRO_CONFORMANCE_ADMIN_USER" "$SYNCHRO_CONFORMANCE_ADMIN_PASSWORD_FILE")
adapter_url=$(make_url "$SYNCHRO_CONFORMANCE_ADAPTER_USER" "$SYNCHRO_CONFORMANCE_ADAPTER_PASSWORD_FILE")
if [ "$SYNCHRO_CONFORMANCE_OPERATOR_USER" = "$SYNCHRO_CONFORMANCE_WORKER_USER" ]; then
  echo "projection bootstrap requires distinct operator and worker users" >&2
  exit 1
fi
operator_url=$(make_url "$SYNCHRO_CONFORMANCE_OPERATOR_USER" "$SYNCHRO_CONFORMANCE_OPERATOR_PASSWORD_FILE")
worker_url=$(make_url "$SYNCHRO_CONFORMANCE_WORKER_USER" "$SYNCHRO_CONFORMANCE_WORKER_PASSWORD_FILE")
admin_password=$(cat "$SYNCHRO_CONFORMANCE_ADMIN_PASSWORD_FILE")
psql_admin() {
  PGPASSWORD="$admin_password" "$pg18_bindir/psql" \
    --dbname "$SYNCHRO_CONFORMANCE_ATTACH_DATABASE_URL" \
    --username "$SYNCHRO_CONFORMANCE_ADMIN_USER" "$@"
}
DATABASE_URL="$admin_url" "$provisioner" prepare --repo-root "$repo_root"

start_adapter() {
  DATABASE_URL="$adapter_url" JWT_SECRET="$(cat "$SYNCHRO_CONFORMANCE_JWT_SECRET_FILE")" LISTEN_ADDR="${listen_url#http://}" "$adapter" >"$work_dir/adapter.log" 2>&1 &
  adapter_pid=$!
  for _ in $(seq 1 60); do curl --fail --silent "$listen_url/ready" >/dev/null && return; sleep 1; done
  cat "$work_dir/adapter.log" >&2
  return 1
}
start_adapter
mkdir -p "$work_dir/protocol-state"
make --no-print-directory -C "$repo_root" server-consumer-smoke-phase \
  SERVER_SMOKE_URL="$listen_url" SERVER_SMOKE_JWT_SECRET_FILE="$SYNCHRO_CONFORMANCE_JWT_SECRET_FILE" \
  SERVER_SMOKE_PHASE=initial SERVER_SMOKE_ADAPTER_PID="$adapter_pid" \
  SERVER_SMOKE_STATE_DIR="$work_dir/protocol-state" SERVER_SMOKE_OUTPUT="$work_dir/initial.json"
killed_pid=$adapter_pid
kill -9 "$adapter_pid"; wait "$adapter_pid" 2>/dev/null || true; adapter_pid=
customer_id=00000000-0000-4000-8000-000000000115
offline_id=00000000-0000-4000-8000-000000000118
# The server must hold the exact first upload and no offline upload before the resume.
before_resume=$(
  psql_admin -XAtq -v ON_ERROR_STOP=1 -v customer_id="$customer_id" -v offline_id="$offline_id" <<'SQL'
SELECT EXISTS (
    SELECT 1 FROM public.customers
    WHERE id = :'customer_id'::uuid AND deleted_at IS NULL AND name = 'Packaged server consumer'
      AND user_id = '00000000-0000-4000-8000-000000000111' AND balance = 0 AND is_active
  )
  AND NOT EXISTS (SELECT 1 FROM public.customers WHERE id = :'offline_id'::uuid);
SQL
)
test "$before_resume" = t || { echo "server does not hold exactly the initial upload before resume" >&2; exit 1; }
remote_name="Server authored $(python3 -c 'import uuid; print(uuid.uuid4())') é世"
authored=$(
  psql_admin -XAtq -v ON_ERROR_STOP=1 -v customer_id="$customer_id" -v remote_name="$remote_name" <<'SQL'
UPDATE public.customers SET name = :'remote_name', updated_at = clock_timestamp()
WHERE id = :'customer_id'::uuid AND name = 'Packaged server consumer'
RETURNING name;
SQL
)
test "$authored" = "$remote_name" || { echo "server did not author exactly one remote value" >&2; exit 1; }
python3 -c 'import json, sys; json.dump({"schema_version": 1, "remote_value": sys.argv[2]}, open(sys.argv[1], "w"))' "$work_dir/remote.json" "$remote_name"
start_adapter
make --no-print-directory -C "$repo_root" server-consumer-smoke-phase \
  SERVER_SMOKE_URL="$listen_url" SERVER_SMOKE_JWT_SECRET_FILE="$SYNCHRO_CONFORMANCE_JWT_SECRET_FILE" \
  SERVER_SMOKE_PHASE=resume SERVER_SMOKE_ADAPTER_PID="$adapter_pid" \
  SERVER_SMOKE_STATE_DIR="$work_dir/protocol-state" SERVER_SMOKE_OUTPUT="$work_dir/resume.json"
after_resume=$(
  psql_admin -XAtq -v ON_ERROR_STOP=1 -v customer_id="$customer_id" -v offline_id="$offline_id" -v remote_name="$remote_name" <<'SQL'
SELECT EXISTS (
    SELECT 1 FROM public.customers
    WHERE id = :'customer_id'::uuid AND deleted_at IS NULL AND name = :'remote_name'
      AND user_id = '00000000-0000-4000-8000-000000000111' AND balance = 0 AND is_active
  )
  AND EXISTS (
    SELECT 1 FROM public.customers
    WHERE id = :'offline_id'::uuid AND deleted_at IS NULL AND name = 'Packaged server offline'
      AND user_id = '00000000-0000-4000-8000-000000000111' AND balance = 0 AND is_active
  );
SQL
)
test "$after_resume" = t || { echo "server does not hold exactly the resumed upload and the remote value" >&2; exit 1; }
python3 -c 'import json, sys; json.dump({"schema_version": 1, "status": "passed", "remote_value": sys.argv[2], "resumed_write": {"customer_id": sys.argv[3], "customer_name": "Packaged server offline"}}, open(sys.argv[1], "w"))' "$work_dir/server.json" "$remote_name" "$offline_id"
bootstrap_row_id=00000000-0000-4000-8000-000000009501
psql_admin -Xq -v ON_ERROR_STOP=1 -v bootstrap_row_id="$bootstrap_row_id" >/dev/null <<'SQL'
INSERT INTO public.cf_late_registration (id, owner_id, value)
VALUES (:'bootstrap_row_id', 'diagnostic-user', 'packaged-projection-bootstrap');

SELECT synchro.synchro_register_table(
  'public.cf_late_registration',
  'public.cf_late_registration_membership',
  'single_scope',
  'id', 'updated_at', 'deleted_at', 'enabled'
);
SQL
registry_generation=$(
  psql_admin -XAtq -v ON_ERROR_STOP=1 <<'SQL'
SELECT registry.registry_generation
FROM synchro.sync_registry registry
JOIN synchro.sync_registry_generations generation
  ON generation.generation = registry.registry_generation
WHERE generation.state = 'pending'
  AND generation.validated
  AND registry.physical_schema = 'public'
  AND registry.physical_relation = 'cf_late_registration'
  AND NOT EXISTS (
    SELECT 1
    FROM synchro.sync_schema_manifest manifest
    WHERE manifest.registry_generation = registry.registry_generation
  )
ORDER BY registry.registry_generation DESC
LIMIT 1;
SQL
)
case "$registry_generation" in
  ''|*[!0-9]*) echo "projection bootstrap registry generation is invalid" >&2; exit 1 ;;
esac
DATABASE_URL="$operator_url" WORKER_DATABASE_URL="$worker_url" \
  timeout 120 "$adapter" projection-bootstrap --registry-generation "$registry_generation" \
  >"$work_dir/projection-bootstrap.json"
test -s "$work_dir/projection-bootstrap.json"
bootstrap_valid=$(
  psql_admin -XAtq -v ON_ERROR_STOP=1 \
    -v registry_generation="$registry_generation" -v bootstrap_row_id="$bootstrap_row_id" <<'SQL'
WITH target AS (
  SELECT registry.relation_id
  FROM synchro.sync_registry registry
  WHERE registry.registry_generation = :'registry_generation'::bigint
    AND registry.physical_schema = 'public'
    AND registry.physical_relation = 'cf_late_registration'
)
SELECT EXISTS (
    SELECT 1
    FROM synchro.sync_registry_generations generation
    WHERE generation.generation = :'registry_generation'::bigint
      AND generation.state = 'active'
      AND generation.validated
  )
  AND EXISTS (
    SELECT 1
    FROM synchro.sync_schema_manifest manifest
    WHERE manifest.registry_generation = :'registry_generation'::bigint
  )
  AND EXISTS (
    SELECT 1
    FROM synchro.sync_stream_resets reset
    WHERE reset.operation_kind = 'projection_bootstrap'
      AND reset.target_registry_generation = :'registry_generation'::bigint
      AND reset.lifecycle = 'cleanup_complete'
  )
  AND EXISTS (
    SELECT 1
    FROM public.cf_late_registration source
    WHERE source.id = :'bootstrap_row_id'::uuid
      AND source.owner_id = 'diagnostic-user'
      AND source.value = 'packaged-projection-bootstrap'
  )
  AND EXISTS (
    SELECT 1
    FROM synchro.sync_captured_rows captured
    JOIN target ON target.relation_id = captured.relation_id
    WHERE captured.record_id = :'bootstrap_row_id'
      AND NOT captured.deleted
  )
  AND EXISTS (
    SELECT 1
    FROM synchro.sync_bucket_edges edge
    JOIN target ON target.relation_id = edge.relation_id
    WHERE edge.record_id = :'bootstrap_row_id'
      AND edge.bucket_id = 'user:diagnostic-user'
  );
SQL
)
if [ "$bootstrap_valid" != t ]; then
  echo "projection bootstrap did not complete cleanup and preserve its published row and scope state" >&2
  exit 1
fi
DATABASE_URL="$admin_url" "$seed" --output "$work_dir/seed.sqlite"
test -s "$work_dir/seed.sqlite"
set -- python3 "$tool" complete-server-cell --repo-root "$repo_root" --cell "$cell_id" --output "$cell_result" --initial "$work_dir/initial.json" --resume "$work_dir/resume.json" --killed-pid "$killed_pid" --remote "$work_dir/remote.json" --server-verification "$work_dir/server.json" --artifact "$extension_archive" --artifact "$adapter" --artifact "$seed"
for hash in $sealed_hashes; do set -- "$@" --expected-artifact-hash "$hash"; done
"$@"
"$provisioner" lifecycle --state-dir "$work_dir/state" destroy "$attach_run_id" >/dev/null
destroyed=1
wait "$provisioner_pid"
provisioner_pid=
