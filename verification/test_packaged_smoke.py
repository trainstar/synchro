#!/usr/bin/env python3
"""Run structural controls for packaged smoke evidence."""

from __future__ import annotations

import copy
import base64
import fcntl
import hashlib
import hmac
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from pathlib import Path
from unittest import mock

sys.dont_write_bytecode = True
from verification import packaged_smoke


REPO_ROOT = Path(__file__).resolve().parents[1]


class PackagedSmokeStructureTests(unittest.TestCase):
    def test_generated_credential_covers_build_and_bounded_job(self) -> None:
        issued_at = 1_800_000_000
        secret = "fixture-signing-secret"
        with mock.patch.dict(os.environ, {"SYNCHRO_TEST_JWT_SECRET": secret}, clear=True):
            with mock.patch.object(packaged_smoke.time, "time", return_value=issued_at):
                token = packaged_smoke.bearer_token("package-user")
        header, payload, signature = token.split(".")
        claims = json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))
        self.assertEqual(claims["sub"], "package-user")
        self.assertEqual(claims["iat"], issued_at)
        self.assertLess(issued_at + 65 * 60, claims["exp"])
        self.assertEqual(claims["exp"], issued_at + 6 * 60 * 60)
        expected_signature = hmac.new(
            secret.encode(), f"{header}.{payload}".encode(), hashlib.sha256
        ).digest()
        self.assertEqual(signature, packaged_smoke.base64url(expected_signature))
        with mock.patch.dict(os.environ, {"SYNCHRO_PACKAGED_SMOKE_TOKEN": "supplied-token"}, clear=True):
            self.assertEqual(packaged_smoke.bearer_token("package-user"), "supplied-token")

    def start_app_result_collector(
        self,
        directory: Path,
    ) -> tuple[packaged_smoke.AppResultHTTPServer, str]:
        server = packaged_smoke.create_app_result_server(directory)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()

        def stop() -> None:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)

        self.addCleanup(stop)
        host, port = server.server_address
        return server, f"http://{host}:{port}{packaged_smoke.APP_RESULT_PATH}"

    def post_app_result(
        self,
        url: str,
        token: str,
        value: object,
    ) -> int:
        request = urllib.request.Request(
            url,
            data=json.dumps(value).encode("utf-8"),
            method="POST",
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                return response.status
        except urllib.error.HTTPError as error:
            try:
                return error.code
            finally:
                error.close()

    def post_raw_app_result(
        self,
        url: str,
        token: str,
        value: bytes,
    ) -> int:
        request = urllib.request.Request(
            url,
            data=value,
            method="POST",
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                return response.status
        except urllib.error.HTTPError as error:
            try:
                return error.code
            finally:
                error.close()

    def test_summary_rejects_self_consistent_but_wrong_release_hashes(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            manifest_path = root / "release-manifest.json"
            roles = ["pg-extension", "adapter", "seed-tool", "kotlin-maven", "react-native-npm"]
            hashes = {role: str(index + 1) * 64 for index, role in enumerate(roles)}
            packaged_smoke.write_json(manifest_path, {
                "source": {"commit": packaged_smoke.source_commit(REPO_ROOT)},
                "distributions": [
                    {"role": role, "kind": "file", "sha256": digest}
                    for role, digest in hashes.items()
                ],
            })
            manifest_hash = packaged_smoke.hash_files([manifest_path])[0]
            cells = {
                "SUP-PG-LINUX-X64-001": [hashes[role] for role in roles[:3]],
                "SUP-IOS-MIN-001": [manifest_hash],
                "SUP-IOS-CURRENT-001": [manifest_hash],
                "SUP-ANDROID-MIN-001": [hashes["kotlin-maven"]],
                "SUP-ANDROID-CURRENT-001": [hashes["kotlin-maven"]],
                "SUP-RN-IOS-CURRENT-001": [manifest_hash, hashes["react-native-npm"]],
                "SUP-RN-ANDROID-CURRENT-001": [hashes["kotlin-maven"], hashes["react-native-npm"]],
            }
            summary = {
                "schema_version": 1,
                "source_commit": packaged_smoke.source_commit(REPO_ROOT),
                "artifact_hashes": sorted({h for hs in cells.values() for h in hs}),
                "status": "passed",
                "obligations": [
                    {"id": f"smoke/{cell}/{operation}", "kind": "smoke", "status": "passed",
                     "terminal": True, "test_count": 1, "artifact_hashes": hs}
                    for cell, hs in cells.items() for operation in packaged_smoke.SMOKE_OPERATIONS
                ],
            }
            summary_path = root / "summary.json"
            packaged_smoke.write_json(summary_path, summary)
            packaged_smoke.verify_summary(REPO_ROOT, summary_path, manifest_path)
            summary["obligations"][0]["artifact_hashes"] = ["f" * 64]
            summary["artifact_hashes"].append("f" * 64)
            packaged_smoke.write_json(summary_path, summary)
            with self.assertRaisesRegex(packaged_smoke.EvidenceError, "does not match sealed"):
                packaged_smoke.verify_summary(REPO_ROOT, summary_path, manifest_path)

    def test_dry_summary_and_mutations_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory(prefix="packaged-smoke-structure.") as raw_directory:
            directory = Path(raw_directory)
            dry_path = directory / "dry.json"
            packaged_smoke.dry_summary(REPO_ROOT, dry_path)
            dry = packaged_smoke.load_json(dry_path, "dry summary")
            self.assertIsInstance(dry, dict)

            with self.assertRaisesRegex(packaged_smoke.EvidenceError, "invalid members"):
                packaged_smoke.verify_summary(REPO_ROOT, dry_path)

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

    def test_smoke_config_reads_private_collector_configuration(self) -> None:
        environment = {
            "SYNCHRO_TEST_URL": "http://127.0.0.1:8080",
            "SYNCHRO_PACKAGED_SMOKE_TOKEN": "server-token",
        }
        with tempfile.TemporaryDirectory(prefix="packaged-smoke-config.") as raw_directory:
            directory = Path(raw_directory)
            ready = directory / "collector.json"
            output = directory / "config.json"
            collector = {
                "schema_version": 1,
                "url": "http://127.0.0.1:9876/result",
                "token": "t" * 43,
            }
            packaged_smoke.write_json(ready, collector, mode=0o600)
            with mock.patch.dict(os.environ, environment):
                ordinary = packaged_smoke.smoke_config("SUP-IOS-MIN-001", "ios")
                self.assertNotIn("result_url", ordinary)
                self.assertNotIn("result_token", ordinary)
                packaged_smoke.write_config(
                    "SUP-RN-IOS-CURRENT-001",
                    "react-native-ios",
                    output,
                    ready,
                )
            collected = packaged_smoke.load_json(output, "collected config")
            self.assertEqual(collected["result_url"], collector["url"])
            self.assertEqual(collected["result_token"], collector["token"])

            ready.chmod(0o644)
            with mock.patch.dict(os.environ, environment):
                with self.assertRaisesRegex(packaged_smoke.EvidenceError, "mode 0600"):
                    packaged_smoke.write_config(
                        "SUP-RN-IOS-CURRENT-001",
                        "react-native-ios",
                        output,
                        ready,
                    )

    def test_extra_cell_evidence_fails(self) -> None:
        with tempfile.TemporaryDirectory(prefix="packaged-smoke-extra-cell.") as raw_directory:
            directory = Path(raw_directory)
            cells = directory / "cells"
            cells.mkdir()
            (cells / "unexpected.json").write_text("{}", encoding="utf-8")
            with self.assertRaisesRegex(packaged_smoke.EvidenceError, "unexpected cell evidence"):
                packaged_smoke.collect_summary(REPO_ROOT, cells, directory / "summary.json")

    def test_process_replacement_is_required(self) -> None:
        with tempfile.TemporaryDirectory(prefix="packaged-smoke-lifecycle.") as raw_directory:
            directory = Path(raw_directory)
            initial = directory / "initial.json"
            resume = directory / "resume.json"
            artifact = directory / "artifact.bin"
            output = directory / "cell.json"
            packaged_smoke.write_json(initial, {
                "schema_version": 1, "phase": "initial", "status": "passed",
                "pid": 101, "pending_change_count": 1,
            })
            packaged_smoke.write_json(resume, {
                "schema_version": 1, "phase": "resume", "status": "passed",
                "pid": 202, "pending_change_count": 0,
            })
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
                [packaged_smoke.hash_files([artifact])[0]],
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

            packaged_smoke.write_json(resume, {
                "schema_version": 1, "phase": "resume", "status": "passed",
                "pid": 101, "pending_change_count": 0,
            })
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
                    [packaged_smoke.hash_files([artifact])[0]],
                )

    def test_app_result_collector_rejects_wrong_identity_and_conflicts(self) -> None:
        with tempfile.TemporaryDirectory(prefix="packaged-smoke-app-result.") as raw_directory:
            directory = Path(raw_directory) / "results"
            server, url = self.start_app_result_collector(directory)
            initial = {
                "schema_version": 1,
                "phase": "initial",
                "status": "passed",
                "pending_change_count": 1,
                "error": None,
            }
            resume = {
                "schema_version": 1,
                "phase": "resume",
                "status": "passed",
                "pending_change_count": 0,
                "error": None,
            }
            self.assertEqual(self.post_app_result(url, f"wrong-{server.token}", initial), 401)
            self.assertEqual(self.post_app_result(url, "\u00e9" * 43, initial), 401)
            self.assertEqual(self.post_app_result(url, server.token, resume), 409)
            self.assertEqual(
                self.post_app_result(url, server.token, {**initial, "unexpected": True}),
                400,
            )
            self.assertEqual(
                self.post_app_result(url, server.token, {**initial, "padding": "x" * 5000}),
                413,
            )
            self.assertEqual(
                self.post_raw_app_result(
                    url,
                    server.token,
                    b'{"schema_version":1,"schema_version":1,"phase":"initial","status":"passed","pending_change_count":1,"error":null}',
                ),
                400,
            )
            for malformed in (
                {**initial, "schema_version": True},
                {**initial, "phase": []},
                {**initial, "status": {}},
            ):
                self.assertEqual(self.post_app_result(url, server.token, malformed), 400)
            self.assertEqual(self.post_app_result(url, server.token, initial), 201)
            self.assertEqual(
                self.post_app_result(
                    url,
                    server.token,
                    {**initial, "status": "failed", "pending_change_count": 0, "error": "late failure"},
                ),
                409,
            )
            self.assertEqual(
                packaged_smoke.load_json(directory / "initial.json", "initial result"),
                initial,
            )

    def test_app_result_collector_requires_fresh_result_directory(self) -> None:
        with tempfile.TemporaryDirectory(prefix="packaged-smoke-app-stale.") as raw_directory:
            directory = Path(raw_directory) / "results"
            directory.mkdir()
            packaged_smoke.write_json(directory / "initial.json", {
                "schema_version": 1,
                "phase": "initial",
                "status": "passed",
                "pending_change_count": 1,
                "error": None,
            })
            with self.assertRaisesRegex(packaged_smoke.EvidenceError, "must be fresh"):
                packaged_smoke.create_app_result_server(directory)

    def test_app_result_collector_bounds_connection_reads(self) -> None:
        with tempfile.TemporaryDirectory(prefix="packaged-smoke-app-timeout.") as raw_directory:
            directory = Path(raw_directory) / "results"
            with mock.patch.object(packaged_smoke, "APP_RESULT_READ_TIMEOUT_SECONDS", 0.05):
                server, _ = self.start_app_result_collector(directory)
                host, port = server.server_address
                with socket.create_connection((host, port), timeout=5) as connection:
                    request = (
                        "POST /result HTTP/1.1\r\n"
                        f"Host: {host}:{port}\r\n"
                        f"Authorization: Bearer {server.token}\r\n"
                        "Content-Type: application/json\r\n"
                        "Content-Length: 100\r\n"
                        "\r\n"
                    )
                    connection.sendall(request.encode("ascii"))
                    response = connection.recv(4096)
            self.assertIn(b"408 Request Timeout", response)
            self.assertFalse((directory / "initial.json").exists())

    def test_failed_app_completion_after_empty_queue_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory(prefix="packaged-smoke-app-failure.") as raw_directory:
            directory = Path(raw_directory) / "results"
            server, url = self.start_app_result_collector(directory)
            initial = {
                "schema_version": 1,
                "phase": "initial",
                "status": "passed",
                "pending_change_count": 1,
                "error": None,
            }
            failed_resume = {
                "schema_version": 1,
                "phase": "resume",
                "status": "failed",
                "pending_change_count": 0,
                "error": "client close failed",
            }
            self.assertEqual(self.post_app_result(url, server.token, initial), 201)
            self.assertEqual(self.post_app_result(url, server.token, failed_resume), 201)
            output = directory / "resume-phase.json"
            with self.assertRaisesRegex(packaged_smoke.EvidenceError, "application reported failure"):
                packaged_smoke.await_app_result(
                    directory / "resume.json",
                    "resume",
                    202,
                    output,
                    0.1,
                )
            self.assertFalse(output.exists())

    def test_missing_app_result_fails_within_its_bound(self) -> None:
        with tempfile.TemporaryDirectory(prefix="packaged-smoke-app-missing.") as raw_directory:
            directory = Path(raw_directory)
            with self.assertRaisesRegex(packaged_smoke.EvidenceError, "phase result is missing"):
                packaged_smoke.await_app_result(
                    directory / "initial.json",
                    "initial",
                    101,
                    directory / "phase.json",
                    0.01,
                )
            for timeout in (float("nan"), float("inf"), float("-inf")):
                with self.assertRaisesRegex(packaged_smoke.EvidenceError, "outside the supported bound"):
                    packaged_smoke.await_app_result(
                        directory / "initial.json",
                        "initial",
                        101,
                        directory / "phase.json",
                        timeout,
                    )

    def test_truthful_app_results_keep_counts_and_receive_host_pids(self) -> None:
        with tempfile.TemporaryDirectory(prefix="packaged-smoke-app-success.") as raw_directory:
            directory = Path(raw_directory) / "results"
            with mock.patch.object(socket, "getfqdn", side_effect=AssertionError("collector requires reverse DNS")):
                server, url = self.start_app_result_collector(directory)
            self.assertGreaterEqual(len(server.token), 32)
            initial = {
                "schema_version": 1,
                "phase": "initial",
                "status": "passed",
                "pending_change_count": 1,
                "error": None,
            }
            resume = {
                "schema_version": 1,
                "phase": "resume",
                "status": "passed",
                "pending_change_count": 0,
                "error": None,
            }
            self.assertEqual(self.post_app_result(url, server.token, initial), 201)
            self.assertEqual(self.post_app_result(url, server.token, resume), 201)
            initial_phase = directory / "initial-phase.json"
            resume_phase = directory / "resume-phase.json"
            packaged_smoke.await_app_result(
                directory / "initial.json", "initial", 101, initial_phase, 0.1,
            )
            packaged_smoke.await_app_result(
                directory / "resume.json", "resume", 202, resume_phase, 0.1,
            )
            self.assertEqual(
                packaged_smoke.load_json(initial_phase, "initial phase"),
                {
                    "schema_version": 1,
                    "phase": "initial",
                    "status": "passed",
                    "pid": 101,
                    "pending_change_count": 1,
                },
            )
            self.assertEqual(
                packaged_smoke.load_json(resume_phase, "resume phase"),
                {
                    "schema_version": 1,
                    "phase": "resume",
                    "status": "passed",
                    "pid": 202,
                    "pending_change_count": 0,
                },
            )

    def test_required_cells_exclude_tested_development_hosts(self) -> None:
        cells = packaged_smoke.required_cells(REPO_ROOT)
        self.assertNotIn("SUP-MACOS-CURRENT-001", cells)
        self.assertEqual(len(cells), 7)

    def test_wrong_artifact_hash_fails(self) -> None:
        with tempfile.TemporaryDirectory(prefix="packaged-smoke-hash.") as raw_directory:
            directory = Path(raw_directory)
            initial = directory / "initial.json"
            resume = directory / "resume.json"
            artifact = directory / "artifact.bin"
            packaged_smoke.write_json(initial, {
                "schema_version": 1, "phase": "initial", "status": "passed",
                "pid": 101, "pending_change_count": 1,
            })
            packaged_smoke.write_json(resume, {
                "schema_version": 1, "phase": "resume", "status": "passed",
                "pid": 202, "pending_change_count": 0,
            })
            artifact.write_bytes(b"packaged artifact")
            with self.assertRaisesRegex(packaged_smoke.EvidenceError, "do not match sealed"):
                packaged_smoke.complete_cell(
                    REPO_ROOT,
                    packaged_smoke.required_cells(REPO_ROOT)[0],
                    directory / "cell.json",
                    initial,
                    resume,
                    101,
                    [artifact],
                    ["0" * 64],
                )

    def test_server_completion_rejects_changed_digest_and_equal_pid(self) -> None:
        with tempfile.TemporaryDirectory(prefix="packaged-smoke-server.") as raw_directory:
            directory = Path(raw_directory)
            artifact = directory / "artifact"
            artifact.write_bytes(b"sealed")
            digest = "a" * 64
            initial = {"schema_version": 1, "phase": "initial", "status": "passed", "adapter_pid": 101, "push_digest": digest}
            resume = {"schema_version": 1, "phase": "resume", "status": "passed", "adapter_pid": 202, "push_digest": digest, "replay_equal": True}
            initial_path, resume_path = directory / "initial.json", directory / "resume.json"
            cell_path = directory / "cell.json"
            packaged_smoke.write_json(initial_path, initial)
            packaged_smoke.write_json(resume_path, resume)
            packaged_smoke.complete_server_cell(
                REPO_ROOT,
                packaged_smoke.required_cells(REPO_ROOT)[0],
                cell_path,
                initial_path,
                resume_path,
                101,
                [artifact],
                packaged_smoke.hash_files([artifact]),
            )
            cell = packaged_smoke.load_json(cell_path, "server cell")
            self.assertEqual(cell["process_lifecycle"]["kind"], "server")
            packaged_smoke.validate_cell(cell, packaged_smoke.required_cells(REPO_ROOT)[0], packaged_smoke.source_commit(REPO_ROOT))

            resume["push_digest"] = "b" * 64
            packaged_smoke.write_json(resume_path, resume)
            with self.assertRaisesRegex(packaged_smoke.EvidenceError, "replay digest"):
                packaged_smoke.complete_server_cell(REPO_ROOT, packaged_smoke.required_cells(REPO_ROOT)[0], directory / "cell.json", initial_path, resume_path, 101, [artifact], packaged_smoke.hash_files([artifact]))
            resume["push_digest"] = digest
            resume["adapter_pid"] = 101
            packaged_smoke.write_json(resume_path, resume)
            with self.assertRaisesRegex(packaged_smoke.EvidenceError, "process replacement"):
                packaged_smoke.complete_server_cell(REPO_ROOT, packaged_smoke.required_cells(REPO_ROOT)[0], directory / "cell.json", initial_path, resume_path, 101, [artifact], packaged_smoke.hash_files([artifact]))



FAKE_SWIFT = """#!/bin/sh
case "$1" in
  package) printf '{}' ;;
  build)
    while [ "$#" -gt 0 ]; do
      if [ "$1" = --scratch-path ]; then scratch=$2; fi
      shift
    done
    mkdir -p "$scratch/debug"
    cp "$FAKE_CONSUMER" "$scratch/debug/SynchroConsumer"
    chmod +x "$scratch/debug/SynchroConsumer"
    ;;
  *) exit 64 ;;
esac
"""

# The fake consumer holds an exclusive lock for its whole life. A free lock
# after the script returns proves that no consumer survived, without a
# process ID that the operating system could reuse.
FAKE_CONSUMER = """#!/usr/bin/env python3
import fcntl, json, os, time
lock = open(os.environ["FAKE_CONSUMER_LOCK"] + "." + os.environ["SYNCHRO_PACKAGED_SMOKE_PHASE"], "w")
fcntl.flock(lock, fcntl.LOCK_EX)
open(lock.name + ".started", "w").close()
phase = os.environ["SYNCHRO_PACKAGED_SMOKE_PHASE"]
if os.environ["FAKE_CONSUMER_HANG"] != phase:
    with open(os.environ["SYNCHRO_PACKAGED_SMOKE_PHASE_RESULT"], "w") as result:
        json.dump({"phase": phase, "pid": os.getpid()}, result)
    if phase == "resume":
        raise SystemExit(0)
while True:
    time.sleep(60)
"""

FAKE_SMOKE_TOOL = """import json, sys
arguments = sys.argv[2:]
output = arguments[arguments.index("--output") + 1]
if sys.argv[1] == "complete-cell":
    initial = json.load(open(arguments[arguments.index("--initial") + 1]))
    if int(arguments[arguments.index("--killed-pid") + 1]) != initial["pid"]:
        raise SystemExit("killed pid differs from the initial consumer")
with open(output, "w") as target:
    json.dump({"command": sys.argv[1]}, target)
"""


class SwiftConsumerLifecycleTests(unittest.TestCase):
    phase_seconds = 2
    script_bound_seconds = 30

    def run_consumer(self, hang: str) -> tuple[subprocess.CompletedProcess[str], Path]:
        directory = Path(tempfile.mkdtemp(prefix="swift-consumer-lifecycle."))
        self.addCleanup(shutil.rmtree, directory, True)
        root = directory / "repo"
        (root / "verification").mkdir(parents=True)
        (root / "VERSION").write_text("0.0.0\n", encoding="utf-8")
        (root / "verification" / "packaged_smoke.py").write_text(FAKE_SMOKE_TOOL, encoding="utf-8")
        artifacts = directory / "artifacts"
        (artifacts / "apple" / "Synchro").mkdir(parents=True)
        (artifacts / "apple" / "Synchro" / "Package.swift").write_text("", encoding="utf-8")
        (artifacts / "apple" / "synchro-spm-0.0.0.tar.gz").write_bytes(b"")
        bin_directory = directory / "bin"
        bin_directory.mkdir()
        (bin_directory / "swift").write_text(FAKE_SWIFT, encoding="utf-8")
        (bin_directory / "swift").chmod(0o755)
        consumer = directory / "consumer.py"
        consumer.write_text(FAKE_CONSUMER, encoding="utf-8")
        environment = {
            **os.environ,
            "PATH": f"{bin_directory}{os.pathsep}{os.environ['PATH']}",
            "FAKE_CONSUMER": str(consumer),
            "FAKE_CONSUMER_LOCK": str(directory / "consumer.lock"),
            "FAKE_CONSUMER_HANG": hang,
            "PACKAGED_SMOKE_TMP_ROOT": str(directory / "tmp"),
            "PACKAGED_SMOKE_PHASE_SECONDS": str(self.phase_seconds),
            "PACKAGED_SMOKE_EXPECTED_ARTIFACT_HASHES": "unused",
        }
        command = [
            "sh",
            str(REPO_ROOT / "verification" / "consumers" / "swift" / "test-consumer.sh"),
            str(root),
            str(artifacts),
            "cell",
            str(directory / "cell.json"),
        ]
        # The script leads its own process group, and that group stays owned
        # until communicate reaps the script, so the timeout path can stop it.
        process = subprocess.Popen(
            command, env=environment, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, start_new_session=True
        )
        try:
            stdout, stderr = process.communicate(timeout=self.script_bound_seconds)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            process.communicate()
            self.fail("Swift consumer script did not return within its bound")
        return subprocess.CompletedProcess(command, process.returncode, stdout, stderr), directory

    def assert_no_consumer_survives(self, directory: Path, phases: tuple[str, ...]) -> None:
        for phase in phases:
            lock_path = directory / f"consumer.lock.{phase}"
            self.assertTrue(Path(f"{lock_path}.started").exists(), f"{phase} consumer did not start")
            with open(lock_path, "w") as lock:
                try:
                    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    self.fail(f"{phase} consumer survived the script")
        self.assertEqual(list((directory / "tmp").iterdir()), [])

    def test_killed_initial_phase_and_resume_complete_the_cell(self) -> None:
        result, directory = self.run_consumer(hang="none")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads((directory / "cell.json").read_text(encoding="utf-8")), {"command": "complete-cell"})
        self.assert_no_consumer_survives(directory, ("initial", "resume"))

    def test_hung_initial_phase_fails_without_a_surviving_consumer(self) -> None:
        result, directory = self.run_consumer(hang="initial")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("initial phase did not become ready", result.stderr)
        self.assertFalse((directory / "cell.json").exists())
        self.assert_no_consumer_survives(directory, ("initial",))

    def test_hung_resume_phase_fails_without_a_surviving_consumer(self) -> None:
        result, directory = self.run_consumer(hang="resume")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("resume phase did not pass", result.stderr)
        self.assertFalse((directory / "cell.json").exists())
        self.assert_no_consumer_survives(directory, ("initial", "resume"))

if __name__ == "__main__":
    unittest.main()
