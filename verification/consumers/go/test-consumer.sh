#!/bin/sh
set -eu

repo_root=${1:?repository root is required}
version=${2:?release version is required}
mode=${SYNCHRO_GO_CONSUMER_MODE:-public}
consumer_root=$repo_root/verification/consumers/go

case "$mode" in
  public)
    : "${GOPROXY:?GOPROXY is required for public Go consumption}"
    ;;
  prepublication)
    : "${SYNCHRO_GO_PREPUBLICATION_GIT_URL:?SYNCHRO_GO_PREPUBLICATION_GIT_URL is required}"
    GOPROXY=direct
    GONOSUMDB=github.com/trainstar/synchro
    GOPRIVATE=github.com/trainstar/synchro
    GIT_ALLOW_PROTOCOL=file:https
    GIT_CONFIG_COUNT=1
    GIT_CONFIG_KEY_0="url.${SYNCHRO_GO_PREPUBLICATION_GIT_URL}.insteadOf"
    GIT_CONFIG_VALUE_0=https://github.com/trainstar/synchro
    export GOPROXY GONOSUMDB GOPRIVATE GIT_ALLOW_PROTOCOL
    export GIT_CONFIG_COUNT GIT_CONFIG_KEY_0 GIT_CONFIG_VALUE_0
    ;;
  *) printf '%s\n' "unsupported Go consumer mode: $mode" >&2; exit 1 ;;
esac

case "$GOPROXY" in
  *"$repo_root/api/go"*)
    printf '%s\n' "Go consumer proxy resolves repository source" >&2
    exit 1
    ;;
esac

work_dir=$(mktemp -d "${TMPDIR:-/tmp}/synchro-go-consumer.XXXXXX")
trap 'rm -rf "$work_dir"' EXIT HUP INT TERM
GOMODCACHE="$work_dir/modules"
GOCACHE="$work_dir/build-cache"
GOENV=off
GOFLAGS=-modcacherw
GIT_CONFIG_GLOBAL=/dev/null
GIT_CONFIG_NOSYSTEM=1
export GOMODCACHE GOCACHE GOENV GOFLAGS GIT_CONFIG_GLOBAL GIT_CONFIG_NOSYSTEM
cp "$consumer_root/go.mod" "$consumer_root/main.go" "$work_dir/"
(cd "$work_dir" && GOWORK=off go mod edit -require="github.com/trainstar/synchro/api/go@v$version")
(cd "$work_dir" && GOWORK=off go mod tidy)
(cd "$work_dir" && GOWORK=off go list -m -json github.com/trainstar/synchro/api/go > "$work_dir/module.json")
resolved=$(python3 -c 'import json, sys; print(json.load(open(sys.argv[1]))["Version"])' "$work_dir/module.json")
test "$resolved" = "v$version"
(cd "$work_dir" && GOWORK=off go build ./...)
