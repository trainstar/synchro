#!/usr/bin/env python3
"""Unit tests for gate-variable receipt recording and validation."""

from __future__ import annotations

import importlib.util
import sys
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


if __name__ == "__main__":
    unittest.main()
