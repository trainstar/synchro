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

package_artifacts() {
	workspace=$1
	label=$2
	artifact_root="$run_root/artifacts/$label"
	target_root="$run_root/targets/$label"
	adapter_artifact="$artifact_root/adapter"
	extension_artifact="$artifact_root/extension"
	adapter_log="$logs_root/$label-adapter-package.log"
	extension_log="$logs_root/$label-extension-package.log"

	if ! make --no-print-directory -s -C "$workspace" conformance-adapter-artifact \
		CONFORMANCE_ADAPTER_ARTIFACT_DIR="$adapter_artifact" >"$adapter_log" 2>&1; then
		fail "adapter packaging failed for $label"
	fi
	if ! make --no-print-directory -s -C "$workspace" conformance-pg18-extension-artifact \
		PGRX_TARGET_DIR="$target_root" \
		CONFORMANCE_EXTENSION_ARTIFACT="$extension_artifact" >"$extension_log" 2>&1; then
		fail "extension packaging failed for $label"
	fi

	PACKAGE_EXTENSION_ARTIFACT=$extension_artifact
	PACKAGE_ADAPTER_ARTIFACT="$adapter_artifact/synchrod-pg"
}

run_control() {
	label=$1
	phase=$2
	workspace=$3
	test_name=$4
	extension_artifact=$5
	adapter_artifact=$6
	json_log="$logs_root/$label-$phase.json"
	stderr_log="$logs_root/$label-$phase.stderr.log"
	parser_output="$logs_root/$label-$phase.parser.out"
	parser_log="$logs_root/$label-$phase.parser.stderr.log"

	set +e
	env \
		SYNCHRO_CONFORMANCE_EXTENSION_ARTIFACT="$extension_artifact" \
		SYNCHRO_CONFORMANCE_ADAPTER_ARTIFACT="$adapter_artifact" \
		make --no-print-directory -s -C "$workspace" test-blackbox-mutation-control \
		MUTATION_CONTROL_TEST="$test_name" >"$json_log" 2>"$stderr_log"
	control_status=$?
	set -e

	set +e
	make --no-print-directory -s -C "$repo_root" parse-testresult \
		TESTRESULT_TEST_NAME="$test_name" <"$json_log" >"$parser_output" 2>"$parser_log"
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
	test_name=$4
	extension_artifact=$5
	adapter_artifact=$6
	expected=$7

	run_control "$label" "$phase" "$workspace" "$test_name" "$extension_artifact" "$adapter_artifact"
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
	test_name=$3
	workspace="$run_root/workspaces/$category"

	copy_worktree "$workspace"
	apply_mutation "$workspace" "$patch"
	package_artifacts "$workspace" "$category"
	mutant_extension=$PACKAGE_EXTENSION_ARTIFACT
	mutant_adapter=$PACKAGE_ADAPTER_ARTIFACT

	expect_control_result \
		"$category" baseline "$repo_root" "$test_name" \
		"$baseline_extension" "$baseline_adapter" target_pass
	expect_control_result \
		"$category" mutant "$workspace" "$test_name" \
		"$mutant_extension" "$mutant_adapter" target_semantic_test_failure
	expect_control_result \
		"$category" post-baseline "$repo_root" "$test_name" \
		"$baseline_extension" "$baseline_adapter" target_pass
	cleanup_category "$category"
	printf 'KILLED %s by %s\n' "$category" "$test_name"
}

printf '%s\n' 'Packaging unmodified integration mutation artifacts'
package_artifacts "$repo_root" baseline
baseline_extension=$PACKAGE_EXTENSION_ARTIFACT
baseline_adapter=$PACKAGE_ADAPTER_ARTIFACT

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

gate_passed=1
printf '%s\n' 'Integration mutation gate passed: 5 killed, 0 survived'
