#!/usr/bin/env python3
"""Build closed evidence input from terminal gate receipts and artifact files."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
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
SMOKE_OPERATIONS = ("connect", "push", "pull", "kill", "resume")
GATE_TARGET = re.compile(r"^phase-5-check:\s*(.*)$", re.MULTILINE)


class InputError(ValueError):
    pass


def load_json(path: Path, label: str) -> object:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise InputError(f"{label} is missing or malformed: {error}") from error


def required_obligations(repo_root: Path) -> dict[str, str]:
    matrix = load_json(repo_root / "conformance/support-matrix.json", "support matrix")
    if not isinstance(matrix, dict):
        raise InputError("support matrix is not an object")
    semantic_cells = matrix.get("semantic_corpus_cell_ids")
    cells = matrix.get("cells")
    if not isinstance(semantic_cells, list) or not isinstance(cells, list):
        raise InputError("support matrix does not declare semantic cells and cells")
    expected: dict[str, str] = {}
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
            cell = obligation.get("support_cell_id")
            if cell not in semantic_cells:
                continue
            obligation_id = obligation.get("obligation_id")
            if not isinstance(obligation_id, str) or not obligation_id:
                raise InputError(f"scenario {path} has an invalid obligation id")
            key = f"semantic/{obligation_id}"
            if key in expected:
                raise InputError(f"duplicate semantic obligation {key}")
            expected[key] = "semantic"
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


def read_receipts(root: Path, expected: dict[str, str], hashes: list[str]) -> list[dict[str, object]]:
    if not root.is_dir():
        raise InputError(f"gate result directory is missing: {root}")
    receipts: dict[str, dict[str, object]] = {}
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
        count = value.get("test_count")
        if not isinstance(count, int) or isinstance(count, bool) or count < 1:
            raise InputError(f"gate result {gate} has no positive test count")
        receipts[gate] = value
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
            obligations[obligation_id] = item
    missing = sorted(set(expected) - obligations.keys())
    if missing:
        raise InputError(f"missing terminal obligations: {', '.join(missing)}")
    for obligation_id, item in obligations.items():
        if item.get("kind") != expected[obligation_id] or item.get("status") != "passed" or item.get("terminal") is not True:
            raise InputError(f"obligation {obligation_id} is not a terminal pass")
        count = item.get("test_count")
        if not isinstance(count, int) or isinstance(count, bool) or count < 1:
            raise InputError(f"obligation {obligation_id} has no positive test count")
        item["artifact_hashes"] = hashes
    return [obligations[key] for key in sorted(obligations)]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, required=True)
    parser.add_argument("--results-dir", type=Path, required=True)
    parser.add_argument("--artifact-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        expected = required_obligations(args.repo_root)
        hashes = artifact_hashes(args.artifact_root)
        obligations = read_receipts(args.results_dir, expected, hashes)
        variables = []
        for name in GATE_VARIABLES:
            if name not in os.environ:
                raise InputError(f"missing required gate variable: {name}")
            value = os.environ[name]
            if len(value) > 4096 or any(character in value for character in "\x00\r\n"):
                raise InputError(f"gate variable {name} has an unsafe value")
            variables.append({"name": name, "value": value})
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
