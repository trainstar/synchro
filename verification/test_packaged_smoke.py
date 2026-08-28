#!/usr/bin/env python3
"""Run structural controls for packaged smoke evidence."""

from __future__ import annotations

import copy
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

sys.dont_write_bytecode = True
import packaged_smoke


REPO_ROOT = Path(__file__).resolve().parents[1]
CHECKER = REPO_ROOT / "scripts/release-support-check.py"


class PackagedSmokeStructureTests(unittest.TestCase):
    def run_checker(self, summary: Path) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [
                "python3",
                str(CHECKER),
                "--repo-root",
                str(REPO_ROOT),
                "--evidence",
                str(summary),
                "--kind",
                "smoke",
            ],
            text=True,
            capture_output=True,
            check=False,
        )

    def test_dry_summary_and_mutations_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory(prefix="packaged-smoke-structure.") as raw_directory:
            directory = Path(raw_directory)
            dry_path = directory / "dry.json"
            packaged_smoke.dry_summary(REPO_ROOT, dry_path)
            dry = packaged_smoke.load_json(dry_path, "dry summary")
            self.assertIsInstance(dry, dict)

            result = self.run_checker(dry_path)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("dry-run summary is not release evidence", result.stderr)

            missing_cell = copy.deepcopy(dry)
            first_cell = packaged_smoke.required_cells(REPO_ROOT)[0]
            missing_cell["obligations"] = [
                item
                for item in missing_cell["obligations"]
                if not item["id"].startswith(f"smoke/{first_cell}/")
            ]
            missing_cell_path = directory / "missing-cell.json"
            packaged_smoke.write_json(missing_cell_path, missing_cell)
            result = self.run_checker(missing_cell_path)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("missing obligations", result.stderr)

            missing_operation = copy.deepcopy(dry)
            del missing_operation["obligations"][0]
            missing_operation_path = directory / "missing-operation.json"
            packaged_smoke.write_json(missing_operation_path, missing_operation)
            result = self.run_checker(missing_operation_path)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("missing obligations", result.stderr)

            nonterminal = copy.deepcopy(dry)
            nonterminal["obligations"][0]["terminal"] = False
            nonterminal_path = directory / "nonterminal.json"
            packaged_smoke.write_json(nonterminal_path, nonterminal)
            result = self.run_checker(nonterminal_path)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("is not terminal", result.stderr)

    def test_missing_cells_become_terminal_failures(self) -> None:
        with tempfile.TemporaryDirectory(prefix="packaged-smoke-collect.") as raw_directory:
            directory = Path(raw_directory)
            output = directory / "summary.json"
            packaged_smoke.collect_summary(REPO_ROOT, directory / "cells", output)
            summary = packaged_smoke.load_json(output, "collected summary")
            self.assertEqual(summary["status"], "failed")
            expected_count = len(packaged_smoke.required_cells(REPO_ROOT)) * len(
                packaged_smoke.SMOKE_OPERATIONS
            )
            self.assertEqual(len(summary["obligations"]), expected_count)
            self.assertTrue(all(item["terminal"] is True for item in summary["obligations"]))
            self.assertTrue(all(item["status"] == "failed" for item in summary["obligations"]))

    def test_process_replacement_is_required(self) -> None:
        with tempfile.TemporaryDirectory(prefix="packaged-smoke-lifecycle.") as raw_directory:
            directory = Path(raw_directory)
            initial = directory / "initial.json"
            resume = directory / "resume.json"
            artifact = directory / "artifact.bin"
            output = directory / "cell.json"
            packaged_smoke.write_phase(initial, "initial", 101, 1)
            packaged_smoke.write_phase(resume, "resume", 202, 0)
            artifact.write_bytes(b"packaged artifact")

            cell_id = packaged_smoke.required_cells(REPO_ROOT)[0]
            packaged_smoke.complete_cell(
                REPO_ROOT,
                cell_id,
                output,
                initial,
                resume,
                101,
                [artifact],
            )
            cell = packaged_smoke.load_json(output, "completed cell")
            self.assertEqual(cell["status"], "passed")

            extra_member = copy.deepcopy(cell)
            extra_member["unexpected"] = True
            with self.assertRaisesRegex(packaged_smoke.EvidenceError, "invalid members"):
                packaged_smoke.validate_cell(
                    extra_member,
                    cell_id,
                    packaged_smoke.source_commit(REPO_ROOT),
                )

            packaged_smoke.write_phase(resume, "resume", 101, 0)
            with self.assertRaisesRegex(
                packaged_smoke.EvidenceError,
                "resume reused the killed consumer process",
            ):
                packaged_smoke.complete_cell(
                    REPO_ROOT,
                    cell_id,
                    output,
                    initial,
                    resume,
                    101,
                    [artifact],
                )


if __name__ == "__main__":
    unittest.main()
