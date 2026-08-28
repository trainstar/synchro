#!/usr/bin/env bash
set -euo pipefail

: "${DATABASE_URL:?DATABASE_URL is required}"
: "${JWT_SECRET:?JWT_SECRET is required}"
: "${LISTEN_ADDR:?LISTEN_ADDR is required}"
: "${SYNCHROD_ADAPTER_BINARY:?SYNCHROD_ADAPTER_BINARY is required}"
: "${SYNCHROD_ADAPTER_PID_FILE:?SYNCHROD_ADAPTER_PID_FILE is required}"
: "${SYNCHROD_ADAPTER_LOG_FILE:?SYNCHROD_ADAPTER_LOG_FILE is required}"

test -x "$SYNCHROD_ADAPTER_BINARY"
mkdir -p "$(dirname "$SYNCHROD_ADAPTER_PID_FILE")" "$(dirname "$SYNCHROD_ADAPTER_LOG_FILE")"

if [ -f "$SYNCHROD_ADAPTER_PID_FILE" ]; then
  pid="$(cat "$SYNCHROD_ADAPTER_PID_FILE")"
  if kill -0 "$pid" 2>/dev/null; then
    printf '%s\n' "synchrod adapter already running (pid $pid)"
    exit 0
  fi
  rm -f "$SYNCHROD_ADAPTER_PID_FILE"
fi

nohup env \
  DATABASE_URL="$DATABASE_URL" \
  JWT_SECRET="$JWT_SECRET" \
  LISTEN_ADDR="$LISTEN_ADDR" \
  "$SYNCHROD_ADAPTER_BINARY" >"$SYNCHROD_ADAPTER_LOG_FILE" 2>&1 < /dev/null &
pid=$!
printf '%s\n' "$pid" > "$SYNCHROD_ADAPTER_PID_FILE"

cleanup_failed_start() {
  if kill -0 "$pid" 2>/dev/null; then
    kill "$pid" 2>/dev/null || true
  fi
  rm -f "$SYNCHROD_ADAPTER_PID_FILE"
}
trap cleanup_failed_start EXIT HUP INT TERM

ready_url="${SYNCHROD_ADAPTER_READY_URL:-}"
if [ -z "$ready_url" ]; then
  listen_host="$LISTEN_ADDR"
  case "$listen_host" in
    :*) listen_host="localhost$listen_host" ;;
  esac
  ready_url="http://$listen_host/sync/schema"
fi
for _ in $(seq 1 "${SYNCHROD_ADAPTER_READY_ATTEMPTS:-30}"); do
  if curl --fail --silent --show-error --output /dev/null "$ready_url"; then
    trap - EXIT HUP INT TERM
    printf '%s\n' "synchrod adapter is ready at $ready_url"
    exit 0
  fi
  sleep 1
done

printf '%s\n' "synchrod adapter did not become ready" >&2
cat "$SYNCHROD_ADAPTER_LOG_FILE" >&2 || true
exit 1
