from __future__ import annotations

import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import unittest
import urllib.request

from scripts.ci import adapter_process


REPO_ROOT = Path(__file__).resolve().parents[2]
FIXTURE_SERVER = """#!/usr/bin/env python3
import http.server, os
from pathlib import Path
Path(os.environ["FIXTURE_PID"]).write_text(str(os.getpid()))
if os.environ.get("FIXTURE_EXIT"):
    raise SystemExit(7)
class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(503 if os.environ.get("FIXTURE_UNREADY") else 200)
        self.end_headers()
    def log_message(self, *args):
        pass
server = http.server.HTTPServer(("127.0.0.1", int(os.environ["LISTEN_ADDR"].rsplit(":", 1)[1])), Handler)
server.serve_forever()
"""


class AdapterProcessTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.binary = self.root / "adapter"
        self.binary.write_text(FIXTURE_SERVER)
        self.binary.chmod(0o700)
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            self.port = listener.getsockname()[1]
        self.state = self.root / "adapter.json"
        self.environment = {
            **os.environ,
            "DATABASE_URL": "postgres://fixture/unused",
            "JWT_SECRET": "fixture-only",
            "LISTEN_ADDR": f"127.0.0.1:{self.port}",
            "SYNCHROD_ADAPTER_BINARY": str(self.binary),
            "SYNCHROD_ADAPTER_PID_FILE": str(self.state),
            "SYNCHROD_ADAPTER_LOG_FILE": str(self.root / "adapter.log"),
            "SYNCHROD_ADAPTER_READY_ATTEMPTS": "2",
            "FIXTURE_PID": str(self.root / "child.pid"),
        }
        self.environment.pop("SYNCHROD_ADAPTER_READY_URL", None)
        self.addCleanup(self.stop_owned_fixture)

    def invoke(self, operation: str, environment: dict | None = None) -> subprocess.CompletedProcess:
        return subprocess.run(
            [sys.executable, "-B", "-m", "scripts.ci.adapter_process", operation],
            cwd=REPO_ROOT,
            env=environment or self.environment,
            capture_output=True,
            text=True,
            timeout=30,
        )

    def stop_owned_fixture(self) -> None:
        if not self.state.exists():
            return
        try:
            state = adapter_process.read_state(self.state)
        except RuntimeError:
            return
        if state is not None:
            completed = self.invoke("stop")
            self.assertEqual(completed.returncode, 0, completed.stderr)

    def assert_child_exited(self) -> None:
        pid = int((self.root / "child.pid").read_text())
        with self.assertRaises(ProcessLookupError):
            os.kill(pid, 0)

    def test_start_restart_and_authenticated_stop(self) -> None:
        self.assertEqual(self.invoke("status").returncode, 3)
        for _ in range(2):
            started = self.invoke("start")
            self.assertEqual(started.returncode, 0, started.stderr)
            self.assertEqual(self.invoke("status").returncode, 0)
            state = adapter_process.read_state(self.state)
            self.assertIsNotNone(state)
            assert state is not None
            first = adapter_process.request(state, "status")
            self.assertEqual(self.invoke("start").returncode, 0)
            self.assertEqual(adapter_process.request(state, "status")["pid"], first["pid"])
            wrong = {**state, "run_id": "0" * 64}
            with self.assertRaises(RuntimeError):
                adapter_process.request(wrong, "stop")
            with urllib.request.urlopen(f"http://127.0.0.1:{self.port}/sync/schema", timeout=2) as response:
                self.assertEqual(response.status, 200)
            stopped = self.invoke("stop")
            self.assertEqual(stopped.returncode, 0, stopped.stderr)
            self.assertFalse(self.state.exists())
            self.assert_child_exited()
        self.assertEqual(self.invoke("stop").returncode, 0)

    def test_occupied_port_preserves_unrelated_listener(self) -> None:
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", self.port))
            listener.listen()
            started = self.invoke("start")
            self.assertNotEqual(started.returncode, 0)
            self.assertFalse(self.state.exists())
            with socket.create_connection(("127.0.0.1", self.port), timeout=2):
                connection, _ = listener.accept()
                connection.close()
            self.assertEqual(self.invoke("stop").returncode, 0)

    def test_stale_pid_cannot_signal_an_unrelated_process(self) -> None:
        sleeper = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"])
        try:
            self.state.write_text(str(sleeper.pid))
            self.assertNotEqual(self.invoke("stop").returncode, 0)
            self.assertIsNone(sleeper.poll())
            self.assertTrue(self.state.exists())
        finally:
            sleeper.terminate()
            sleeper.wait(timeout=5)
            self.state.unlink(missing_ok=True)

    def test_failed_start_cleans_its_child_and_state(self) -> None:
        for failure in ("FIXTURE_EXIT", "FIXTURE_UNREADY"):
            with self.subTest(failure=failure):
                completed = self.invoke("start", {**self.environment, failure: "1"})
                self.assertNotEqual(completed.returncode, 0)
                self.assertFalse(self.state.exists())
                self.assert_child_exited()

    def test_existing_owner_rejects_changed_configuration(self) -> None:
        started = self.invoke("start")
        self.assertEqual(started.returncode, 0, started.stderr)
        changed = {**self.environment, "JWT_SECRET": "different-fixture"}
        self.assertNotEqual(self.invoke("start", changed).returncode, 0)
        self.assertEqual(self.invoke("status").returncode, 0)
        self.binary.write_text(FIXTURE_SERVER + "\n# rebuilt fixture\n")
        self.assertNotEqual(self.invoke("start").returncode, 0)


if __name__ == "__main__":
    unittest.main()
