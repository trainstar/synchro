from __future__ import annotations

from contextlib import redirect_stderr, redirect_stdout
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock

from scripts.ci import download_go_modules


class DownloadGoModulesTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.module = self.root / "module"
        self.module.mkdir()
        (self.module / "go.mod").write_text("module example.test/module\n", encoding="utf-8")
        (self.module / "go.sum").write_text("pinned sum\n", encoding="utf-8")
        self.original = {name: (self.module / name).read_bytes() for name in ("go.mod", "go.sum")}
        self.count = self.root / "count"
        fake_go = self.root / "go"
        fake_go.write_text(
            f"#!{sys.executable}\n"
            "import json, os, pathlib, sys, time\n"
            "count = pathlib.Path(os.environ['FAKE_GO_COUNT'])\n"
            "index = int(count.read_text()) if count.exists() else 0\n"
            "count.write_text(str(index + 1))\n"
            "assert sys.argv[1:4] == ['mod', 'download', '-json']\n"
            "assert len(sys.argv) == 5 and sys.argv[4].startswith('-modfile=')\n"
            "assert pathlib.Path.cwd() == pathlib.Path(os.environ['FAKE_GO_MODULE_DIR'])\n"
            "assert os.environ['GOFLAGS'] == ''\n"
            "assert os.environ['GOWORK'] == 'off'\n"
            "response = json.loads(os.environ['FAKE_GO_RESPONSES'])[index]\n"
            "if response.get('mutate'): pathlib.Path(sys.argv[4].split('=', 1)[1]).with_suffix('.sum').write_text('changed')\n"
            "time.sleep(response.get('sleep', 0))\n"
            "print(response.get('stdout', ''))\n"
            "print(response.get('stderr', ''), file=sys.stderr)\n"
            "sys.exit(response['status'])\n",
            encoding="utf-8",
        )
        fake_go.chmod(0o755)

    def run_download(self, responses: list[dict], deadline: float = 120) -> tuple[int, str, str]:
        environment = {
            "PATH": str(self.root) + os.pathsep + os.environ["PATH"],
            "FAKE_GO_COUNT": str(self.count),
            "FAKE_GO_MODULE_DIR": str(self.module),
            "FAKE_GO_RESPONSES": json.dumps(responses),
        }
        out, err = io.StringIO(), io.StringIO()
        with mock.patch.dict(os.environ, environment), mock.patch.object(
            download_go_modules, "RETRY_DELAYS", (0.01, 0.01)
        ), mock.patch.object(download_go_modules, "DEADLINE_SECONDS", deadline):
            with redirect_stdout(out), redirect_stderr(err):
                status = download_go_modules.download(self.module)
        for name, content in self.original.items():
            self.assertEqual((self.module / name).read_bytes(), content)
        return status, out.getvalue(), err.getvalue()

    def test_transient_failure_then_success(self) -> None:
        status, out, err = self.run_download([
            {
                "status": 1,
                "stdout": json.dumps({"Error": "dial tcp: lookup proxy.golang.org: temporary failure in name resolution"}),
                "stderr": "go: downloading example.test/module v1.0.0",
            },
            {"status": 0, "stdout": '{"Path":"example.test/module"}'},
        ])
        self.assertEqual(status, 0)
        self.assertEqual(self.count.read_text(), "2")
        self.assertIn("attempt 1/3 failed: dns", err)
        self.assertIn("succeeded on attempt 2", out)
        self.assertNotIn("proxy.golang.org", err)

    def test_hard_failure_stops_after_one_attempt(self) -> None:
        status, _, err = self.run_download([
            {"status": 1, "stderr": "x509: certificate signed by unknown authority; 503 Service Unavailable"},
        ])
        self.assertEqual(status, 1)
        self.assertEqual(self.count.read_text(), "1")
        self.assertIn("failed: certificate", err)
        self.assertNotIn("unknown authority", err)

    def test_other_hard_failures_stop_after_one_attempt(self) -> None:
        failures = {
            "checksum": "checksum mismatch",
            "authentication": "HTTP 401 Unauthorized",
            "resolution": "invalid version: unknown revision",
        }
        for category, message in failures.items():
            with self.subTest(category=category):
                self.count.unlink(missing_ok=True)
                status, _, err = self.run_download([
                    {"status": 1, "stdout": json.dumps({"Error": message})},
                ])
                self.assertEqual(status, 1)
                self.assertEqual(self.count.read_text(), "1")
                self.assertIn(f"failed: {category}", err)

    def test_transient_exhaustion_stops_after_three_attempts(self) -> None:
        status, _, err = self.run_download([
            {"status": 1, "stderr": "HTTP 503 Service Unavailable"} for _ in range(3)
        ])
        self.assertEqual(status, 1)
        self.assertEqual(self.count.read_text(), "3")
        self.assertEqual(err.count("failed: service"), 3)

    def test_total_deadline_stops_running_download(self) -> None:
        status, _, err = self.run_download([{"status": 0, "sleep": 2}], deadline=0.5)
        self.assertEqual(status, 1)
        self.assertLessEqual(int(self.count.read_text()) if self.count.exists() else 0, 1)
        self.assertIn("failed: deadline", err)

    def test_unknown_failure_is_not_retried(self) -> None:
        status, _, err = self.run_download([{"status": 1, "stderr": "unclassified failure"}])
        self.assertEqual(status, 1)
        self.assertEqual(self.count.read_text(), "1")
        self.assertIn("failed: unknown", err)

    def test_mixed_module_errors_are_not_retried(self) -> None:
        output = "\n".join([
            json.dumps({"Error": "HTTP 503 Service Unavailable"}),
            json.dumps({"Error": "unclassified failure"}),
        ])
        status, _, err = self.run_download([{"status": 1, "stdout": output}])
        self.assertEqual(status, 1)
        self.assertEqual(self.count.read_text(), "1")
        self.assertIn("failed: unknown", err)

    def test_alternate_lockfile_change_is_rejected(self) -> None:
        status, _, err = self.run_download([{"status": 0, "mutate": True}])
        self.assertEqual(status, 1)
        self.assertEqual(self.count.read_text(), "1")
        self.assertIn("failed: lockfile_changed", err)


if __name__ == "__main__":
    unittest.main()
