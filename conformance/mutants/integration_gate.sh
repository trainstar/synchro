#!/bin/sh

set -eu

repo_root=${1:-}
if [ -z "$repo_root" ] || [ ! -f "$repo_root/Makefile" ]; then
	printf '%s\n' 'integration mutation gate requires the repository root' >&2
	exit 1
fi

scratch_parent=${SYNCHRO_MUTANT_TMPDIR:-${TMPDIR:-/tmp}}
run_root=$(mktemp -d "$scratch_parent/synchro-integration-mutants.XXXXXX")
log_parent=${SYNCHRO_MUTANT_LOG_TMPDIR:-${TMPDIR:-/tmp}}
logs_root=$(mktemp -d "$log_parent/synchro-integration-mutant-logs.XXXXXX")
gate_passed=0
mutant_count=0

cleanup() {
	status=$?
	trap - EXIT HUP INT TERM
	rm -rf "$run_root" || :
	if [ "$gate_passed" -eq 1 ]; then
		rm -rf "$logs_root" || :
	else
		printf 'Integration mutation failure logs: %s\n' "$logs_root" >&2
	fi
	exit "$status"
}

trap cleanup EXIT
trap 'exit 1' HUP INT TERM

fail() {
	printf '%s\n' "$*" >&2
	exit 1
}

copy_worktree() {
	destination=$1
	archive="$run_root/current-worktree.tar"
	mkdir -p "$destination"
	(
		cd "$repo_root"
		git ls-files --cached --others --exclude-standard -z |
			perl -0ne 'chomp; print "$_\0" if -e $_ || -l $_' |
			tar --null -T - -cf "$archive"
	)
	tar -C "$destination" -xf "$archive"
	rm -f "$archive"
}

apply_mutation() {
	workspace=$1
	patch=$2
	if ! git -C "$workspace" apply --check "$repo_root/$patch"; then
		fail "integration mutant patch is stale: $patch"
	fi
	git -C "$workspace" apply "$repo_root/$patch"
}

configure_real_environment() {
	pgrx_config=${PGRX_PG_CONFIG:-}
	if [ -z "$pgrx_config" ]; then
		pgrx_config=$(awk -F '"' '/^pg18[[:space:]]*=/ { print $2; exit }' "$HOME/.pgrx/config.toml")
	fi
	if [ ! -x "$pgrx_config" ]; then
		fail "integration mutation gate requires a PostgreSQL 18 pgrx configuration"
	fi

	pg_bindir=$(dirname "$pgrx_config")
	secrets_root="$run_root/secrets"
	mkdir -m 700 "$secrets_root"
	(
		umask 077
		for name in admin adapter observer worker operator jwt; do
			openssl rand -hex 32 >"$secrets_root/$name-password"
		done
	)
}

package_artifacts() {
	workspace=$1
	label=$2
	artifact_root="$run_root/artifacts/$label"
	target_root="$run_root/targets/$label"
	adapter_artifact="$artifact_root/adapter"
	extension_artifact="$artifact_root/extension"
	seed_artifact="$artifact_root/synchro-seed"
	adapter_log="$logs_root/$label-adapter-package.log"
	extension_log="$logs_root/$label-extension-package.log"
	seed_log="$logs_root/$label-seed-package.log"

	if ! make --no-print-directory -s -C "$workspace" conformance-adapter-artifact \
		CONFORMANCE_ADAPTER_ARTIFACT_DIR="$adapter_artifact" >"$adapter_log" 2>&1; then
		fail "adapter packaging failed for $label"
	fi
	if ! make --no-print-directory -s -C "$workspace" conformance-pg18-extension-artifact \
		PGRX_TARGET_DIR="$target_root" \
		CONFORMANCE_EXTENSION_ARTIFACT="$extension_artifact" >"$extension_log" 2>&1; then
		fail "extension packaging failed for $label"
	fi
	if ! make --no-print-directory -s -C "$workspace" conformance-seed-artifact \
		CONFORMANCE_SEED_ARTIFACT="$seed_artifact" >"$seed_log" 2>&1; then
		fail "seed packaging failed for $label"
	fi

	PACKAGE_EXTENSION_ARTIFACT=$extension_artifact
	PACKAGE_ADAPTER_ARTIFACT="$adapter_artifact/synchrod-pg"
	PACKAGE_SEED_ARTIFACT=$seed_artifact
}

run_control() {
	label=$1
	phase=$2
	workspace=$3
	test_path=$4
	extension_artifact=$5
	adapter_artifact=$6
	seed_artifact=$7
	json_log="$logs_root/$label-$phase.json"
	stderr_log="$logs_root/$label-$phase.stderr.log"
	parser_output="$logs_root/$label-$phase.parser.out"
	parser_log="$logs_root/$label-$phase.parser.stderr.log"

	set +e
	env \
		SYNCHRO_CONFORMANCE_PG18_BINDIR="$pg_bindir" \
		SYNCHRO_CONFORMANCE_EXTENSION_ARTIFACT="$extension_artifact" \
		SYNCHRO_CONFORMANCE_ADAPTER_ARTIFACT="$adapter_artifact" \
		SYNCHRO_CONFORMANCE_SEED_ARTIFACT="$seed_artifact" \
		SYNCHRO_CONFORMANCE_ADMIN_USER="synchro_mutant_admin" \
		SYNCHRO_CONFORMANCE_ADMIN_PASSWORD_FILE="$secrets_root/admin-password" \
		SYNCHRO_CONFORMANCE_ADAPTER_USER="synchro_mutant_adapter" \
		SYNCHRO_CONFORMANCE_ADAPTER_PASSWORD_FILE="$secrets_root/adapter-password" \
		SYNCHRO_CONFORMANCE_OBSERVER_USER="synchro_mutant_observer" \
		SYNCHRO_CONFORMANCE_OBSERVER_PASSWORD_FILE="$secrets_root/observer-password" \
		SYNCHRO_CONFORMANCE_WORKER_USER="synchro_mutant_worker" \
		SYNCHRO_CONFORMANCE_WORKER_PASSWORD_FILE="$secrets_root/worker-password" \
		SYNCHRO_CONFORMANCE_OPERATOR_USER="synchro_mutant_operator" \
		SYNCHRO_CONFORMANCE_OPERATOR_PASSWORD_FILE="$secrets_root/operator-password" \
		SYNCHRO_CONFORMANCE_JWT_SECRET_FILE="$secrets_root/jwt-password" \
		SYNCHRO_CONFORMANCE_INSTALL_LOCK="$run_root/install.lock" \
		make --no-print-directory -s -C "$workspace" test-blackbox-mutation-control \
		MUTATION_CONTROL_TEST="$test_path" >"$json_log" 2>"$stderr_log"
	control_status=$?
	set -e

	set +e
	make --no-print-directory -s -C "$repo_root" parse-testresult \
		TESTRESULT_TEST_NAME="$test_path" <"$json_log" >"$parser_output" 2>"$parser_log"
	parser_status=$?
	set -e
	if [ "$parser_status" -ne 0 ]; then
		fail "test-result parser failed for $label $phase"
	fi
	control_result=$(tr -d '\r\n' <"$parser_output")
	CONTROL_STATUS=$control_status
	CONTROL_RESULT=$control_result
}

expect_control_result() {
	label=$1
	phase=$2
	workspace=$3
	test_path=$4
	extension_artifact=$5
	adapter_artifact=$6
	seed_artifact=$7
	expected=$8

	run_control "$label" "$phase" "$workspace" "$test_path" "$extension_artifact" "$adapter_artifact" "$seed_artifact"
	case "$expected" in
		target_pass)
			if [ "$CONTROL_STATUS" -ne 0 ] || [ "$CONTROL_RESULT" != target_pass ]; then
				fail "$label $phase did not pass: status=$CONTROL_STATUS result=$CONTROL_RESULT"
			fi
			;;
		target_semantic_test_failure)
			if [ "$CONTROL_STATUS" -eq 0 ] || [ "$CONTROL_RESULT" != target_semantic_test_failure ]; then
				fail "$label mutant was not killed by its assertion: status=$CONTROL_STATUS result=$CONTROL_RESULT"
			fi
			;;
		*)
			fail "integration mutation gate has an invalid expected result: $expected"
			;;
	esac
}

cleanup_category() {
	category=$1
	workspace_path="$run_root/workspaces/$category"
	artifact_path="$run_root/artifacts/$category"
	target_path="$run_root/targets/$category"
	rm -rf \
		"$workspace_path" \
		"$artifact_path" \
		"$target_path"
	for path in "$workspace_path" "$artifact_path" "$target_path"; do
		if [ -e "$path" ]; then
			fail "integration mutant cleanup failed: $category"
		fi
	done
}

run_category() {
	category=$1
	patch=$2
	test_path=$3
	workspace="$run_root/workspaces/$category"

	copy_worktree "$workspace"
	apply_mutation "$workspace" "$patch"
	package_artifacts "$workspace" "$category"
	mutant_extension=$PACKAGE_EXTENSION_ARTIFACT
	mutant_adapter=$PACKAGE_ADAPTER_ARTIFACT
	mutant_seed=$PACKAGE_SEED_ARTIFACT

	expect_control_result \
		"$category" baseline "$repo_root" "$test_path" \
		"$baseline_extension" "$baseline_adapter" "$baseline_seed" target_pass
	expect_control_result \
		"$category" mutant "$workspace" "$test_path" \
		"$mutant_extension" "$mutant_adapter" "$mutant_seed" target_semantic_test_failure
	expect_control_result \
		"$category" post-baseline "$repo_root" "$test_path" \
		"$baseline_extension" "$baseline_adapter" "$baseline_seed" target_pass
	cleanup_category "$category"
	mutant_count=$((mutant_count + 1))
	printf 'KILLED %s by %s\n' "$category" "$test_path"
}

validate_manifest() {
	if ! make --no-print-directory -s -C "$repo_root" test-integration-mutant-manifest; then
		fail "integration mutant manifest validation failed"
	fi
}

manifest_rows() {
	python3 - "$repo_root/conformance/mutants/integration/manifest.json" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as source:
    manifest = json.load(source)
for mutant in manifest["mutants"]:
    test_path = mutant["test_target"] + "/" + mutant["assertion_subtest"]
    print("\t".join((mutant["id"], mutant["patch"], test_path)))
PY
}

printf '%s\n' 'Packaging unmodified integration mutation artifacts'
validate_manifest
configure_real_environment
package_artifacts "$repo_root" baseline
baseline_extension=$PACKAGE_EXTENSION_ARTIFACT
baseline_adapter=$PACKAGE_ADAPTER_ARTIFACT
baseline_seed=$PACKAGE_SEED_ARTIFACT

run_category \
	cursor-advancement \
	conformance/mutants/integration/cursor-advancement.patch \
	TestRealMutationControlCursorAdvancement
run_category \
	wal-acknowledgment \
	conformance/mutants/integration/wal-acknowledgment.patch \
	TestRealMutationControlWALAcknowledgement
run_category \
	mutation-conservation \
	conformance/mutants/integration/mutation-conservation.patch \
	TestRealMutationControlMutationConservation
run_category \
	checksum-correctness \
	conformance/mutants/integration/checksum-correctness.patch \
	TestRealMutationControlChecksumCorrectness
run_category \
	scope-isolation \
	conformance/mutants/integration/scope-isolation.patch \
	TestRealMutationControlScopeIsolation
run_category \
	progress-order \
	conformance/mutants/integration/progress-order.patch \
	TestRealMutationControlProgressOrder
run_category \
	pull-deduplication \
	conformance/mutants/integration/pull-deduplication.patch \
	TestRealS02DivergentPullPaginationIsStarvationFree

manifest_rows >"$run_root/manifest.tsv"
tab=$(printf '\t')
while IFS="$tab" read -r category patch test_path; do
	run_category "$category" "$patch" "$test_path"
done <"$run_root/manifest.tsv"

gate_passed=1
printf 'Integration mutation gate passed: %s killed, 0 survived\n' "$mutant_count"
