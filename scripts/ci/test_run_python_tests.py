from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


REPO_ROOT = Path(__file__).resolve().parents[2]


class PythonTestGateTests(unittest.TestCase):
    def test_gate_requires_executed_success_without_skips(self) -> None:
        fixtures = {
            "passing": ("import unittest\nclass Case(unittest.TestCase):\n def test_one(self): pass\n", 0, 1, 0),
            "empty": ("import unittest\n", 1, 0, 0),
            "skipped": ("import unittest\nclass Case(unittest.TestCase):\n @unittest.skip('control')\n def test_one(self): pass\n", 1, 1, 1),
            "failed": ("import unittest\nclass Case(unittest.TestCase):\n def test_one(self): self.fail('control')\n", 1, 1, 0),
            "expected_failure": ("import unittest\nclass Case(unittest.TestCase):\n @unittest.expectedFailure\n def test_one(self): self.fail('control')\n", 1, 1, 0),
        }
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            for name, (source, status, count, skipped) in fixtures.items():
                with self.subTest(name=name):
                    module = f"gate_fixture_{name}"
                    (directory / f"{module}.py").write_text(source)
                    environment = os.environ.copy()
                    environment["PYTHONPATH"] = os.pathsep.join(
                        [str(directory), str(REPO_ROOT)]
                    )
                    completed = subprocess.run(
                        [sys.executable, "-B", "-m", "scripts.ci.run_python_tests", module],
                        cwd=REPO_ROOT,
                        env=environment,
                        capture_output=True,
                        text=True,
                        timeout=10,
                    )
                    self.assertEqual(completed.returncode, status, completed.stderr)
                    summary = json.loads(completed.stdout.splitlines()[-1])
                    self.assertEqual(summary["passed"], status == 0)
                    self.assertEqual(summary["tests_run"], count)
                    self.assertEqual(summary["skipped"], skipped)


if __name__ == "__main__":
    unittest.main()
