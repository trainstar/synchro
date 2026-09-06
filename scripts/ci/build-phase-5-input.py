#!/usr/bin/env python3
"""Build closed evidence input from terminal gate receipts and artifact files."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import sys
from pathlib import Path


GATE_VARIABLES = (
    "BLACKBOX_TEST_COUNT",
    "DETOX_ARGS",
    "GO_TEST_ARGS",
    "GO_TEST_PKGS",
    "GRADLE_TEST_ARGS",
    "KOTLIN_ANDROID_SERIAL",
    "MUTATION_CONTROL_EXPECT",
    "MUTATION_CONTROL_TEST",
    "PGRX_TEST_NAME",
    "RN_ANDROID_DETOX_CONFIG",
    "SUPPORT_CELL_ID",
    "SUPPORT_PLATFORM_VERSION",
    "TESTRESULT_TEST_NAME",
)
# Free-form argument variables can carry credentials. Receipts store only digests.
DIGESTED_GATE_VARIABLES = {"DETOX_ARGS", "GO_TEST_ARGS", "GRADLE_TEST_ARGS"}
SHA256_VALUE = re.compile(r"^sha256:[a-f0-9]{64}$")
SHA256_HASH = re.compile(r"^[0-9a-f]{64}$")
COMMIT = re.compile(r"^[0-9a-f]{40}$")
EMBEDDED_CREDENTIAL_URL = re.compile(r"[a-z][a-z0-9+.-]*://[^/\s:@]+:[^/@\s]+@", re.IGNORECASE)
SMOKE_OPERATIONS = ("connect", "push", "pull", "kill", "resume")
GATE_TARGET = re.compile(r"^phase-5-check:\s*(.*)$", re.MULTILINE)


class InputError(ValueError):
    pass


def load_json(path: Path, label: str) -> object:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise InputError(f"{label} is missing or malformed: {error}") from error


def source_commit(repo_root: Path) -> str:
    try:
        commit = subprocess.check_output(
            ["git", "-C", str(repo_root), "rev-parse", "--verify", "HEAD"],
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except (OSError, subprocess.CalledProcessError) as error:
        raise InputError(f"cannot resolve the repository commit: {error}") from error
    if not COMMIT.fullmatch(commit):
        raise InputError("repository HEAD is not a full commit hash")
    return commit


def semantic_obligation_targets(repo_root: Path) -> dict[str, str]:
    matrix = load_json(repo_root / "conformance/support-matrix.json", "support matrix")
    if not isinstance(matrix, dict):
        raise InputError("support matrix is not an object")
    semantic_cells = matrix.get("semantic_corpus_cell_ids")
    if not isinstance(semantic_cells, list):
        raise InputError("support matrix does not declare semantic cells")
    targets: dict[str, str] = {}
    for path in sorted((repo_root / "conformance/scenarios").rglob("*.json")):
        scenario = load_json(path, f"scenario {path}")
        if not isinstance(scenario, dict):
            raise InputError(f"scenario {path} is not an object")
        obligations = scenario.get("proof_obligations", [])
        if not isinstance(obligations, list):
            raise InputError(f"scenario {path} has malformed proof obligations")
        for obligation in obligations:
            if not isinstance(obligation, dict) or obligation.get("proof_type") != "native-e2e":
                continue
            if obligation.get("support_cell_id") not in semantic_cells:
                continue
            obligation_id = obligation.get("obligation_id")
            if not isinstance(obligation_id, str) or not obligation_id:
                raise InputError(f"scenario {path} has an invalid obligation id")
            make_target = obligation.get("make_target")
            if not isinstance(make_target, str) or not make_target:
                raise InputError(f"scenario {path} has no make_target for {obligation_id}")
            key = f"semantic/{obligation_id}"
            if key in targets:
                raise InputError(f"duplicate semantic obligation {key}")
            targets[key] = make_target
    return targets


def required_obligations(repo_root: Path) -> dict[str, str]:
    matrix = load_json(repo_root / "conformance/support-matrix.json", "support matrix")
    if not isinstance(matrix, dict):
        raise InputError("support matrix is not an object")
    semantic_cells = matrix.get("semantic_corpus_cell_ids")
    cells = matrix.get("cells")
    if not isinstance(semantic_cells, list) or not isinstance(cells, list):
        raise InputError("support matrix does not declare semantic cells and cells")
    expected: dict[str, str] = {
        obligation_id: "semantic" for obligation_id in semantic_obligation_targets(repo_root)
    }
    for cell in cells:
        if not isinstance(cell, dict):
            raise InputError("support matrix contains a malformed cell")
        if cell.get("policy") == "excluded":
            continue
        cell_id = cell.get("id")
        if not isinstance(cell_id, str) or not cell_id:
            raise InputError("support matrix contains an invalid cell id")
        for operation in SMOKE_OPERATIONS:
            expected[f"smoke/{cell_id}/{operation}"] = "smoke"
    makefile = (repo_root / "Makefile").read_text(encoding="utf-8")
    match = GATE_TARGET.search(makefile)
    if not match or not match.group(1).strip():
        raise InputError("Makefile has no phase-5-check prerequisites")
    gates = match.group(1).split()
    if len(gates) != len(set(gates)):
        raise InputError("phase-5-check repeats a prerequisite")
    for gate in gates:
        expected[f"gate/{gate}"] = "gate"
    return expected


def artifact_hashes(root: Path) -> list[str]:
    if not root.is_dir():
        raise InputError(f"artifact directory is missing: {root}")
    hashes: list[str] = []
    for path in sorted(path for path in root.rglob("*") if path.is_file()):
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        hashes.append(digest)
    if not hashes:
        raise InputError("artifact directory contains no files")
    return sorted(set(hashes))


def validate_hashes(value: object, label: str, allowed: set[str] | None = None) -> list[str]:
    if not isinstance(value, list) or not value:
        raise InputError(f"{label} must contain at least one artifact hash")
    result: list[str] = []
    for item in value:
        if not isinstance(item, str) or not SHA256_HASH.fullmatch(item):
            raise InputError(f"{label} contains an invalid artifact hash")
        if allowed is not None and item not in allowed:
            raise InputError(f"{label} references an artifact hash outside the assembled artifact set")
        result.append(item)
    if len(result) != len(set(result)):
        raise InputError(f"{label} repeats an artifact hash")
    return result


def read_gate_variables(value: object, gate: str) -> dict[str, str]:
    if not isinstance(value, list) or len(value) != len(GATE_VARIABLES):
        raise InputError(f"gate result {gate} has an incomplete gate-variable set")
    variables: dict[str, str] = {}
    for item in value:
        if not isinstance(item, dict) or set(item) != {"name", "value"}:
            raise InputError(f"gate result {gate} has a malformed gate variable")
        name = item.get("name")
        variable_value = item.get("value")
        if not isinstance(name, str) or name not in GATE_VARIABLES:
            raise InputError(f"gate result {gate} has an unknown gate variable")
        if name in variables:
            raise InputError(f"gate result {gate} repeats gate variable {name}")
        if not isinstance(variable_value, str) or len(variable_value) > 4096 or any(character in variable_value for character in "\x00\r\n"):
            raise InputError(f"gate result {gate} has an unsafe gate variable {name}")
        if EMBEDDED_CREDENTIAL_URL.search(variable_value):
            raise InputError(f"gate result {gate} exposes credentials in gate variable {name}")
        if name in DIGESTED_GATE_VARIABLES and not SHA256_VALUE.fullmatch(variable_value):
            raise InputError(f"gate result {gate} exposes sensitive gate variable {name}")
        variables[name] = variable_value
    return variables


def read_receipts(
    root: Path,
    expected: dict[str, str],
    hashes: list[str],
    expected_commit: str,
) -> tuple[list[dict[str, object]], list[dict[str, str]]]:
    if not root.is_dir():
        raise InputError(f"gate result directory is missing: {root}")
    receipts: dict[str, dict[str, object]] = {}
    receipt_variables: dict[str, dict[str, str]] = {}
    for path in sorted(root.glob("*.json")):
        value = load_json(path, f"gate result {path}")
        if not isinstance(value, dict):
            raise InputError(f"gate result {path} is not an object")
        gate = value.get("gate")
        if not isinstance(gate, str) or gate not in {key[5:] for key in expected if key.startswith("gate/")}:
            raise InputError(f"gate result {path} names an unknown gate")
        if gate in receipts:
            raise InputError(f"gate result {gate} is duplicated")
        if value.get("status") != "passed" or value.get("terminal") is not True:
            raise InputError(f"gate result {gate} is not a terminal pass")
        receipt_commit = value.get("source_commit")
        if not isinstance(receipt_commit, str) or not COMMIT.fullmatch(receipt_commit):
            raise InputError(f"gate result {gate} has a malformed source_commit")
        if receipt_commit != expected_commit:
            raise InputError(f"gate result {gate} source_commit does not match HEAD")
        count = value.get("test_count")
        if not isinstance(count, int) or isinstance(count, bool) or count < 1:
            raise InputError(f"gate result {gate} has no positive test count")
        receipts[gate] = value
        receipt_variables[gate] = read_gate_variables(value.get("gate_variables"), gate)
    gate_names = {key[5:] for key in expected if key.startswith("gate/")}
    missing = sorted(gate_names - receipts.keys())
    if missing:
        raise InputError(f"missing gate results: {', '.join(missing)}")

    obligations: dict[str, dict[str, object]] = {}
    for gate, receipt in receipts.items():
        obligations[f"gate/{gate}"] = {
            "id": f"gate/{gate}",
            "kind": "gate",
            "status": "passed",
            "terminal": True,
            "test_count": receipt["test_count"],
            "artifact_hashes": hashes,
        }
        for item in receipt.get("obligations", []):
            if not isinstance(item, dict):
                raise InputError(f"gate result {gate} contains a malformed obligation")
            obligation_id = item.get("id")
            if not isinstance(obligation_id, str) or obligation_id not in expected:
                raise InputError(f"gate result {gate} contains an unknown obligation")
            if obligation_id in obligations:
                raise InputError(f"obligation {obligation_id} is duplicated")
            if item.get("kind") != expected[obligation_id]:
                raise InputError(f"obligation {obligation_id} has the wrong kind")
            if item.get("status") != "passed" or item.get("terminal") is not True:
                raise InputError(f"obligation {obligation_id} is not a terminal pass")
            count = item.get("test_count")
            if not isinstance(count, int) or isinstance(count, bool) or count < 1:
                raise InputError(f"obligation {obligation_id} has no positive test count")
            item = dict(item)
            item["artifact_hashes"] = validate_hashes(
                item.get("artifact_hashes"),
                f"obligation {obligation_id} artifact_hashes",
                set(hashes),
            )
            obligations[obligation_id] = item
    for obligation_id, item in obligations.items():
        if item.get("kind") != expected[obligation_id] or item.get("status") != "passed" or item.get("terminal") is not True:
            raise InputError(f"obligation {obligation_id} is not a terminal pass")
        count = item.get("test_count")
        if not isinstance(count, int) or isinstance(count, bool) or count < 1:
            raise InputError(f"obligation {obligation_id} has no positive test count")
    variables = []
    for name in GATE_VARIABLES:
        # The JSON object binds each recorded value to the gate that used it.
        by_gate = {gate: receipt_variables[gate][name] for gate in sorted(receipt_variables)}
        variables.append({"name": name, "value": json.dumps(by_gate, sort_keys=True, separators=(",", ":"))})
    return [obligations[key] for key in sorted(obligations)], variables


def read_packaged_smoke_summary(
    path: Path,
    expected: dict[str, str],
    hashes: list[str],
    expected_commit: str,
) -> list[dict[str, object]]:
    summary = load_json(path, "packaged smoke summary")
    if not isinstance(summary, dict):
        raise InputError("packaged smoke summary is not an object")
    if summary.get("schema_version") != 1:
        raise InputError("packaged smoke summary has the wrong schema version")
    summary_commit = summary.get("source_commit")
    if not isinstance(summary_commit, str) or not COMMIT.fullmatch(summary_commit):
        raise InputError("packaged smoke summary has a malformed source_commit")
    if summary_commit != expected_commit:
        raise InputError("packaged smoke summary source_commit does not match HEAD")
    if summary.get("status") != "passed":
        raise InputError("packaged smoke summary status is not passed")
    if summary.get("dry_run") is True:
        raise InputError("dry-run packaged smoke summary is not release evidence")
    assembled_hashes = set(hashes)
    validate_hashes(summary.get("artifact_hashes"), "packaged smoke summary artifact_hashes", assembled_hashes)
    smoke_ids = {key for key, kind in expected.items() if kind == "smoke"}
    records = summary.get("obligations")
    if not isinstance(records, list) or len(records) != len(smoke_ids):
        raise InputError(f"packaged smoke summary must contain exactly {len(smoke_ids)} obligations")
    actual: dict[str, dict[str, object]] = {}
    for record in records:
        if not isinstance(record, dict):
            raise InputError("packaged smoke summary contains a malformed obligation")
        obligation_id = record.get("id")
        if not isinstance(obligation_id, str) or obligation_id not in smoke_ids:
            raise InputError("packaged smoke summary contains an unknown obligation")
        if obligation_id in actual:
            raise InputError(f"packaged smoke summary repeats obligation {obligation_id}")
        if record.get("kind") != "smoke":
            raise InputError(f"obligation {obligation_id} has the wrong kind")
        if record.get("status") != "passed" or record.get("terminal") is not True:
            raise InputError(f"obligation {obligation_id} is not a terminal pass")
        count = record.get("test_count")
        if not isinstance(count, int) or isinstance(count, bool) or count < 1:
            raise InputError(f"obligation {obligation_id} has no positive test count")
        record_hashes = validate_hashes(
            record.get("artifact_hashes"),
            f"obligation {obligation_id} artifact_hashes",
            assembled_hashes,
        )
        actual[obligation_id] = {
            "id": obligation_id,
            "kind": record["kind"],
            "status": record["status"],
            "terminal": record["terminal"],
            "test_count": count,
            "artifact_hashes": record_hashes,
        }
    missing = sorted(smoke_ids - actual.keys())
    if missing:
        raise InputError(f"packaged smoke summary is missing obligations: {', '.join(missing)}")
    return [actual[key] for key in sorted(actual)]


def semantic_obligations(
    targets: dict[str, str],
    gate_obligations: list[dict[str, object]],
    hashes: list[str],
) -> list[dict[str, object]]:
    by_gate = {
        item["id"][len("gate/") :]: item
        for item in gate_obligations
        if isinstance(item.get("id"), str) and item["id"].startswith("gate/")
    }
    result: list[dict[str, object]] = []
    for obligation_id in sorted(targets):
        target = targets[obligation_id]
        receipt = by_gate.get(target)
        if receipt is None:
            raise InputError(
                f"missing receipt for semantic obligation {obligation_id} target {target}"
            )
        result.append(
            {
                "id": obligation_id,
                "kind": "semantic",
                "status": "passed",
                "terminal": True,
                "test_count": receipt["test_count"],
                "artifact_hashes": hashes,
            }
        )
    return result


def validate_packaged_consumer_receipt(
    obligations: list[dict[str, object]], smoke: list[dict[str, object]]
) -> None:
    smoke_by_id = {item["id"]: item for item in smoke}
    embedded = {
        item["id"]: item
        for item in obligations
        if isinstance(item.get("id"), str) and item["id"].startswith("smoke/")
    }
    if set(embedded) != set(smoke_by_id):
        raise InputError("test-packaged-consumers receipt does not embed all smoke obligations")
    for obligation_id, expected in smoke_by_id.items():
        actual = embedded[obligation_id]
        for field in ("kind", "status", "terminal", "test_count", "artifact_hashes"):
            if actual.get(field) != expected.get(field):
                raise InputError(
                    f"test-packaged-consumers receipt does not match smoke obligation {obligation_id}"
                )
    package_gate = next(
        (item for item in obligations if item.get("id") == "gate/test-packaged-consumers"),
        None,
    )
    if package_gate is None:
        raise InputError("test-packaged-consumers receipt is missing")
    expected_count = sum(item["test_count"] for item in smoke)
    if package_gate.get("test_count") != expected_count:
        raise InputError("test-packaged-consumers receipt count does not match smoke operations")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, required=True)
    parser.add_argument("--results-dir", type=Path, required=True)
    parser.add_argument("--artifact-root", type=Path, required=True)
    parser.add_argument("--packaged-smoke-summary", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        repo_root = args.repo_root.resolve()
        expected = required_obligations(repo_root)
        semantic_targets = semantic_obligation_targets(repo_root)
        commit = source_commit(repo_root)
        hashes = artifact_hashes(args.artifact_root)
        receipt_obligations, variables = read_receipts(
            args.results_dir,
            expected,
            hashes,
            commit,
        )
        smoke = read_packaged_smoke_summary(
            args.packaged_smoke_summary,
            expected,
            hashes,
            commit,
        )
        for item in receipt_obligations:
            obligation_id = item["id"]
            if isinstance(obligation_id, str) and not obligation_id.startswith("gate/"):
                if expected[obligation_id] != "smoke":
                    raise InputError(
                        f"receipt supplied non-smoke obligation {obligation_id}"
                    )
        gate_obligations = [
            item
            for item in receipt_obligations
            if isinstance(item.get("id"), str) and item["id"].startswith("gate/")
        ]
        validate_packaged_consumer_receipt(receipt_obligations, smoke)
        obligations = gate_obligations + semantic_obligations(semantic_targets, gate_obligations, hashes) + smoke
        actual_ids = {item["id"] for item in obligations}
        missing = sorted(set(expected) - actual_ids)
        if missing:
            raise InputError(f"missing terminal obligations: {', '.join(missing)}")
        if actual_ids != set(expected):
            raise InputError("assembled obligations contain an unexpected record")
        value = {
            "status": "passed",
            "artifact_hashes": hashes,
            "gate_variables": variables,
            "obligations": obligations,
        }
        args.output.parent.mkdir(parents=True, exist_ok=True)
        temporary = args.output.with_name(f".{args.output.name}.tmp")
        temporary.write_text(json.dumps(value, sort_keys=True, indent=2) + "\n", encoding="utf-8")
        temporary.replace(args.output)
    except (InputError, OSError) as error:
        print(f"build-phase-5-input: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
