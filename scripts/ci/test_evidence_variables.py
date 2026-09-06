#!/usr/bin/env python3
"""Unit tests for gate-variable receipt recording and validation."""

from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path


def load_script(name: str, filename: str):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).with_name(filename))
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {filename}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


capture = load_script("capture_gate_result", "capture-gate-result.py")
builder = load_script("build_phase_5_input", "build-phase-5-input.py")


REPO_ROOT = Path(__file__).resolve().parents[2]


class GateVariableEvidenceTests(unittest.TestCase):
    def environment(self) -> dict[str, str]:
        return {name: f"value-{name}" for name in capture.GATE_VARIABLES}

    def test_capture_records_each_allowlisted_value(self) -> None:
        environment = self.environment()
        records = capture.gate_variables(environment)
        by_name = {record["name"]: record["value"] for record in records}
        self.assertEqual(set(by_name), set(capture.GATE_VARIABLES))
        self.assertEqual(by_name["GO_TEST_PKGS"], environment["GO_TEST_PKGS"])
        for name in capture.DIGESTED_GATE_VARIABLES:
            self.assertRegex(by_name[name], r"^sha256:[a-f0-9]{64}$")

        records = capture.gate_variables({}, ["make", "GO_TEST_PKGS=./seeddb", "test-adapter"])
        by_name = {record["name"]: record["value"] for record in records}
        self.assertEqual(by_name["GO_TEST_PKGS"], "./seeddb")

    def test_missing_and_unlisted_records_fail_closed(self) -> None:
        records = capture.gate_variables(self.environment())
        with self.assertRaisesRegex(builder.InputError, "incomplete"):
            builder.read_gate_variables(records[1:], "test-gate")
        records[0] = {"name": "UNLISTED_VARIABLE", "value": "value"}
        with self.assertRaisesRegex(builder.InputError, "unknown"):
            builder.read_gate_variables(records, "test-gate")

    def test_receipt_commit_mismatch_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory(prefix="gate-receipt.") as raw_directory:
            path = Path(raw_directory) / "test-gate.json"
            path.write_text(
                json.dumps(
                    {
                        "gate": "test-gate",
                        "source_commit": "0" * 40,
                        "status": "passed",
                        "terminal": True,
                        "test_count": 1,
                        "gate_variables": capture.gate_variables(self.environment()),
                    }
                ),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(builder.InputError, "source_commit does not match HEAD"):
                builder.read_receipts(
                    Path(raw_directory),
                    {"gate/test-gate": "gate"},
                    ["a" * 64],
                    builder.source_commit(REPO_ROOT),
                )

    def test_catalog_target_receipt_is_required(self) -> None:
        targets = builder.semantic_obligation_targets(REPO_ROOT)
        obligation_id, target = next(iter(sorted(targets.items())))
        with self.assertRaisesRegex(builder.InputError, "missing receipt for semantic obligation"):
            builder.semantic_obligations(
                {obligation_id: target},
                [],
                ["a" * 64],
            )

    def test_smoke_summary_must_include_every_obligation(self) -> None:
        expected = builder.required_obligations(REPO_ROOT)
        digest = "a" * 64
        smoke_ids = sorted(key for key, kind in expected.items() if kind == "smoke")
        summary = {
            "schema_version": 1,
            "source_commit": builder.source_commit(REPO_ROOT),
            "artifact_hashes": [digest],
            "status": "passed",
            "obligations": [
                {
                    "id": obligation_id,
                    "kind": "smoke",
                    "status": "passed",
                    "terminal": True,
                    "test_count": 1,
                    "artifact_hashes": [digest],
                }
                for obligation_id in smoke_ids
            ],
        }
        with tempfile.TemporaryDirectory(prefix="packaged-smoke-summary.") as raw_directory:
            path = Path(raw_directory) / "summary.json"
            path.write_text(json.dumps(summary), encoding="utf-8")
            records = builder.read_packaged_smoke_summary(
                path,
                expected,
                [digest],
                summary["source_commit"],
            )
            self.assertEqual(len(records), 40)
            del summary["obligations"][0]
            path.write_text(json.dumps(summary), encoding="utf-8")
            with self.assertRaisesRegex(builder.InputError, "exactly 40 obligations"):
                builder.read_packaged_smoke_summary(
                    path,
                    expected,
                    [digest],
                    summary["source_commit"],
                )


if __name__ == "__main__":
    unittest.main()
