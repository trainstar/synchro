#!/usr/bin/env bash
set -euo pipefail

: "${GITHUB_ENV:?GITHUB_ENV is required}"
: "${GITHUB_WORKSPACE:?GITHUB_WORKSPACE is required}"
: "${RUNNER_TEMP:?RUNNER_TEMP is required}"

jwt_secret_file_name="${SYNCHRO_CONFORMANCE_JWT_SECRET_FILE_NAME:-jwt-secret}"
secrets_dir="$RUNNER_TEMP/synchro-conformance-secrets"

umask 077
mkdir "$secrets_dir"
for principal in admin adapter observer worker operator; do
  openssl rand -hex 32 > "$secrets_dir/$principal-password"
done
openssl rand -hex 32 > "$secrets_dir/$jwt_secret_file_name"
chmod 0600 "$secrets_dir"/*

pg_config="$(awk -F'"' '/^pg18[[:space:]]*=/ { print $2 }' "$HOME/.pgrx/config.toml")"
test -x "$pg_config"

{
  echo "SYNCHRO_CONFORMANCE_PG18_BINDIR=$(dirname "$pg_config")"
  echo "SYNCHRO_CONFORMANCE_EXTENSION_ARTIFACT=$GITHUB_WORKSPACE/dist/conformance/synchro-pg-pg18"
  echo "SYNCHRO_CONFORMANCE_ADAPTER_ARTIFACT=$GITHUB_WORKSPACE/dist/conformance/synchrod-pg-adapter/synchrod-pg"
  echo "SYNCHRO_CONFORMANCE_ADMIN_USER=synchro_cf_admin"
  echo "SYNCHRO_CONFORMANCE_ADMIN_PASSWORD_FILE=$secrets_dir/admin-password"
  echo "SYNCHRO_CONFORMANCE_ADAPTER_USER=synchro_cf_adapter"
  echo "SYNCHRO_CONFORMANCE_ADAPTER_PASSWORD_FILE=$secrets_dir/adapter-password"
  echo "SYNCHRO_CONFORMANCE_OBSERVER_USER=synchro_cf_observer"
  echo "SYNCHRO_CONFORMANCE_OBSERVER_PASSWORD_FILE=$secrets_dir/observer-password"
  echo "SYNCHRO_CONFORMANCE_WORKER_USER=synchro_cf_worker"
  echo "SYNCHRO_CONFORMANCE_WORKER_PASSWORD_FILE=$secrets_dir/worker-password"
  echo "SYNCHRO_CONFORMANCE_OPERATOR_USER=synchro_cf_operator"
  echo "SYNCHRO_CONFORMANCE_OPERATOR_PASSWORD_FILE=$secrets_dir/operator-password"
  echo "SYNCHRO_CONFORMANCE_JWT_SECRET_FILE=$secrets_dir/$jwt_secret_file_name"
  echo "SYNCHRO_CONFORMANCE_INSTALL_LOCK=$RUNNER_TEMP/synchro-conformance-install.lock"
} >> "$GITHUB_ENV"
