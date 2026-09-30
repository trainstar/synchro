#!/usr/bin/env python3
"""Run structural controls for packaged smoke evidence."""

from __future__ import annotations

import copy
import base64
import hashlib
import hmac
import json
import os
import socket
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
REMOTE_NAME = "Server authored fixture \u00e9\u4e16"
# Hand-authored from the training dataset rows that the consumer and harness write.
# The apps report each value as text, so the JSON values are JSON text here.
INITIAL_OBSERVED = {
    "exercise_name": "Back Squat",
    "exercise_muscle_groups": '["quadriceps","glutes"]',
    "program_title": 'Packaged Block \u2705 "consumer"',
    "program_settings": '{"deload_week":4}',
    "external_ref": "9007199254740993",
    "total_volume_kg": "1343.25",
    "sets": "2:5:102.5,3:8:110.25",
}
RESUME_OBSERVED = {
    **INITIAL_OBSERVED,
    "program_title": REMOTE_NAME,
    "program_settings": '{"phases": ["base", "peak"], "deload_week": 5}',
    "external_ref": "9007199254740995",
    "total_volume_kg": "1760",
    "sets": "2:5:102.5,3:8:110.25,4:2:120,5:1:125.5",
}


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

    def convergence_records(self, directory: Path, server_name: str = REMOTE_NAME) -> tuple[Path, Path]:
        remote = directory / "remote.json"
        server = directory / "server.json"
        packaged_smoke.write_json(remote, {"schema_version": 1, "remote_value": REMOTE_NAME})
        packaged_smoke.write_json(server, packaged_smoke.server_verification(
            server_name, packaged_smoke.CLIENT_RESUMED_WRITE,
        ))
        return remote, server

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
                "pid": 101, "pending_change_count": 2, "observed": INITIAL_OBSERVED,
            })
            packaged_smoke.write_json(resume, {
                "schema_version": 1, "phase": "resume", "status": "passed",
                "pid": 202, "pending_change_count": 0, "observed": RESUME_OBSERVED,
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
                *self.convergence_records(directory),
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
                "pid": 101, "pending_change_count": 0, "observed": RESUME_OBSERVED,
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
                    *self.convergence_records(directory),
                )

    def test_app_result_collector_rejects_wrong_identity_and_conflicts(self) -> None:
        with tempfile.TemporaryDirectory(prefix="packaged-smoke-app-result.") as raw_directory:
            directory = Path(raw_directory) / "results"
            server, url = self.start_app_result_collector(directory)
            initial = {
                "schema_version": 1,
                "phase": "initial",
                "status": "passed",
                "pending_change_count": 2,
                "observed": INITIAL_OBSERVED,
                "error": None,
            }
            resume = {
                "schema_version": 1,
                "phase": "resume",
                "status": "passed",
                "pending_change_count": 0,
                "observed": RESUME_OBSERVED,
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
                    b'{"schema_version":1,"schema_version":1,"phase":"initial","status":"passed","pending_change_count":1,"observed":null,"error":null}',
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
                    {**initial, "status": "failed", "pending_change_count": 0, "observed": None, "error": "late failure"},
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
                "pending_change_count": 2,
                "observed": INITIAL_OBSERVED,
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
                "pending_change_count": 2,
                "observed": INITIAL_OBSERVED,
                "error": None,
            }
            failed_resume = {
                "schema_version": 1,
                "phase": "resume",
                "status": "failed",
                "pending_change_count": 0,
                "observed": None,
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
                "pending_change_count": 2,
                "observed": INITIAL_OBSERVED,
                "error": None,
            }
            resume = {
                "schema_version": 1,
                "phase": "resume",
                "status": "passed",
                "pending_change_count": 0,
                "observed": RESUME_OBSERVED,
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
                    "pending_change_count": 2,
                    "observed": INITIAL_OBSERVED,
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
                    "observed": RESUME_OBSERVED,
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
                "pid": 101, "pending_change_count": 2, "observed": INITIAL_OBSERVED,
            })
            packaged_smoke.write_json(resume, {
                "schema_version": 1, "phase": "resume", "status": "passed",
                "pid": 202, "pending_change_count": 0, "observed": RESUME_OBSERVED,
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
                    *self.convergence_records(directory),
                )

    def test_convergence_requires_remote_value_and_exact_server_state(self) -> None:
        with tempfile.TemporaryDirectory(prefix="packaged-smoke-convergence.") as raw_directory:
            directory = Path(raw_directory)
            artifact = directory / "artifact.bin"
            artifact.write_bytes(b"packaged artifact")
            initial = directory / "initial.json"
            resume = directory / "resume.json"
            packaged_smoke.write_json(initial, {
                "schema_version": 1, "phase": "initial", "status": "passed",
                "pid": 101, "pending_change_count": 2, "observed": INITIAL_OBSERVED,
            })
            cell_id = packaged_smoke.required_cells(REPO_ROOT)[0]
            for observed, server_name, error in (
                ({**RESUME_OBSERVED, "program_title": INITIAL_OBSERVED["program_title"]}, REMOTE_NAME, "dataset state: row.program_title"),
                ({**RESUME_OBSERVED, "total_volume_kg": "1708.75"}, REMOTE_NAME, "dataset state: row.total_volume_kg"),
                ({**RESUME_OBSERVED, "sets": INITIAL_OBSERVED["sets"]}, REMOTE_NAME, "dataset state: row.sets"),
                ({**RESUME_OBSERVED, "sets": "1:5:100," + RESUME_OBSERVED["sets"]}, REMOTE_NAME, "dataset state: row.sets"),
                ({**RESUME_OBSERVED, "external_ref": "9007199254740996"}, REMOTE_NAME, "dataset state: row.external_ref"),
                ({**RESUME_OBSERVED, "program_settings": '{"deload_week":5,"phases":["peak","base"]}'}, REMOTE_NAME, "dataset state: row.program_settings"),
                ({**RESUME_OBSERVED, "exercise_muscle_groups": '"quadriceps,glutes"'}, REMOTE_NAME, "dataset state: row.exercise_muscle_groups"),
                (RESUME_OBSERVED, INITIAL_OBSERVED["program_title"], "server verification does not confirm"),
            ):
                with self.subTest(error=error):
                    packaged_smoke.write_json(resume, {
                        "schema_version": 1, "phase": "resume", "status": "passed",
                        "pid": 202, "pending_change_count": 0, "observed": observed,
                    })
                    with self.assertRaisesRegex(packaged_smoke.EvidenceError, error):
                        packaged_smoke.complete_cell(
                            REPO_ROOT, cell_id, directory / "cell.json", initial, resume, 101,
                            [artifact], packaged_smoke.hash_files([artifact]),
                            *self.convergence_records(directory, server_name),
                        )
                    self.assertFalse((directory / "cell.json").exists())

    def test_independent_server_checks_require_exact_rows(self) -> None:
        with tempfile.TemporaryDirectory(prefix="packaged-smoke-server-rows.") as raw_directory:
            directory = Path(raw_directory)
            config_path = directory / "config.json"
            config = {
                "schema_version": 1, "user_id": "user-1",
                "program_id": "00000000-0000-4000-8000-000000000001",
                "workout_id": "00000000-0000-4000-8000-000000000002",
                "entry_id": "00000000-0000-4000-8000-000000000003",
                "set_ids": ["00000000-0000-4000-8000-00000000001%d" % index for index in range(3)],
                "remote_set_ids": ["00000000-0000-4000-8000-00000000002%d" % index for index in range(2)],
            }
            packaged_smoke.write_json(config_path, config)
            state_path = directory / "server-state.json"
            fake_psql = directory / "psql"
            # The fake server applies the remote transaction only to the exact authored title.
            fake_psql.write_text(f"#!{sys.executable}\n" + (
                "import json, sys\n"
                "from pathlib import Path\n"
                "args = sys.argv[1:]\n"
                "assert args[:2] == ['--dbname', 'postgresql://fixture'], args\n"
                "variables = dict(item.split('=', 1) for item in args[args.index('-v') + 1::2])\n"
                f"path = Path({str(state_path)!r})\n"
                "state = json.loads(path.read_text())\n"
                "sql = sys.stdin.read()\n"
                "assert variables['program_id'] == state['program_id'] and variables['entry_id'] == state['entry_id']\n"
                "if sql.startswith('SELECT json_build_object'):\n"
                "    print(json.dumps(state['row']))\n"
                "elif sql.startswith('BEGIN'):\n"
                "    assert all(set_id in sql for set_id in state['remote_set_ids']), sql\n"
                "    if state['row']['program']['title'] == variables['authored_title']:\n"
                "        state['row']['program']['title'] = variables['remote_title']\n"
                "        state['row']['program']['settings'] = json.loads(variables['remote_settings'])\n"
                "        state['row']['workout']['external_ref'] = variables['remote_external_ref']\n"
                "        state['row']['workout']['total_volume_kg'] = '1708.75'\n"
                "        print(variables['remote_title'])\n"
                "        print(variables['remote_external_ref'])\n"
                "        print('1708.75')\n"
                "    path.write_text(json.dumps(state))\n"
            ), encoding="utf-8")
            fake_psql.chmod(0o755)

            def server_state(title: str, durable: bool, remote: bool, total: str, deleted: bool | None = None) -> None:
                row = packaged_smoke.expected_server_rows(
                    config, title, packaged_smoke.consumer_sets(config, durable=durable, remote=remote), total, remote,
                )
                if deleted is not None:
                    row["sets"][0]["deleted"] = deleted
                packaged_smoke.write_json(state_path, {
                    "program_id": config["program_id"], "entry_id": config["entry_id"],
                    "remote_set_ids": config["remote_set_ids"], "row": row,
                })

            authored_title = INITIAL_OBSERVED["program_title"]
            environment = {"ADAPTER_TEST_URL": "postgresql://fixture", "PACKAGED_SMOKE_PSQL": str(fake_psql)}
            remote = directory / "remote.json"
            server = directory / "server.json"
            with mock.patch.dict(os.environ, environment):
                server_state(authored_title, durable=True, remote=False, total="1343.25")
                with self.assertRaisesRegex(packaged_smoke.EvidenceError, r"exactly the initial upload before resume: row.sets\[0\].deleted, row.sets\[2\].reps$"):
                    packaged_smoke.author_remote_value(config_path, remote)
                self.assertFalse(remote.exists())

                server_state(authored_title, durable=False, remote=False, total="1343.25")
                packaged_smoke.author_remote_value(config_path, remote)
                remote_title = packaged_smoke.load_remote_value(remote)
                self.assertNotEqual(remote_title, authored_title)
                self.assertEqual(packaged_smoke.load_json(state_path, "state")["row"]["program"]["title"], remote_title)

                # The remote rows are present, but the resumed upload is absent.
                server_state(remote_title, durable=False, remote=True, total="1708.75")
                with self.assertRaisesRegex(packaged_smoke.EvidenceError, "resumed upload and the remote value: row.sets\\[0\\].deleted, row.sets\\[2\\].reps, row.workout.total_volume_kg$"):
                    packaged_smoke.verify_server_state(config_path, remote, server)
                # The resumed upload is present, but the rollup did not include it.
                server_state(remote_title, durable=True, remote=True, total="1708.75")
                with self.assertRaisesRegex(packaged_smoke.EvidenceError, "resumed upload and the remote value: row.workout.total_volume_kg"):
                    packaged_smoke.verify_server_state(config_path, remote, server)
                # The update is uploaded, but the soft delete is absent.
                server_state(remote_title, durable=True, remote=True, total="2260", deleted=False)
                with self.assertRaisesRegex(packaged_smoke.EvidenceError, "resumed upload and the remote value: row.sets\\[0\\].deleted, row.workout.total_volume_kg$"):
                    packaged_smoke.verify_server_state(config_path, remote, server)
                self.assertFalse(server.exists())

                server_state(remote_title, durable=True, remote=True, total="1760")
                packaged_smoke.verify_server_state(config_path, remote, server)
                packaged_smoke.validate_server_verification(
                    server, remote_title, {"set_index": 3, "reps": 8, "deleted_set_index": 1},
                )

    def test_server_completion_rejects_changed_digest_and_equal_pid(self) -> None:
        with tempfile.TemporaryDirectory(prefix="packaged-smoke-server.") as raw_directory:
            directory = Path(raw_directory)
            artifact = directory / "artifact"
            artifact.write_bytes(b"sealed")
            digest = "a" * 64
            initial = {"schema_version": 1, "phase": "initial", "status": "passed", "adapter_pid": 101, "push_digest": digest}
            resume = {
                "schema_version": 1, "phase": "resume", "status": "passed", "adapter_pid": 202,
                "push_digest": digest, "replay_equal": True, "observed_customer_name": REMOTE_NAME,
            }
            remote = directory / "remote.json"
            server = directory / "server.json"
            packaged_smoke.write_json(remote, {"schema_version": 1, "remote_value": REMOTE_NAME})
            packaged_smoke.write_json(server, packaged_smoke.server_verification(REMOTE_NAME, packaged_smoke.SERVER_OFFLINE_WRITE))
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
                remote,
                server,
            )
            cell = packaged_smoke.load_json(cell_path, "server cell")
            self.assertEqual(cell["process_lifecycle"]["kind"], "server")
            packaged_smoke.validate_cell(cell, packaged_smoke.required_cells(REPO_ROOT)[0], packaged_smoke.source_commit(REPO_ROOT))

            resume["push_digest"] = "b" * 64
            packaged_smoke.write_json(resume_path, resume)
            with self.assertRaisesRegex(packaged_smoke.EvidenceError, "replay digest"):
                packaged_smoke.complete_server_cell(REPO_ROOT, packaged_smoke.required_cells(REPO_ROOT)[0], directory / "cell.json", initial_path, resume_path, 101, [artifact], packaged_smoke.hash_files([artifact]), remote, server)
            resume["push_digest"] = digest
            resume["adapter_pid"] = 101
            packaged_smoke.write_json(resume_path, resume)
            with self.assertRaisesRegex(packaged_smoke.EvidenceError, "process replacement"):
                packaged_smoke.complete_server_cell(REPO_ROOT, packaged_smoke.required_cells(REPO_ROOT)[0], directory / "cell.json", initial_path, resume_path, 101, [artifact], packaged_smoke.hash_files([artifact]), remote, server)
            resume["adapter_pid"] = 202
            resume["observed_customer_name"] = "Packaged server consumer"
            packaged_smoke.write_json(resume_path, resume)
            with self.assertRaisesRegex(packaged_smoke.EvidenceError, "did not pull the remote value"):
                packaged_smoke.complete_server_cell(REPO_ROOT, packaged_smoke.required_cells(REPO_ROOT)[0], directory / "cell.json", initial_path, resume_path, 101, [artifact], packaged_smoke.hash_files([artifact]), remote, server)
            resume["observed_customer_name"] = REMOTE_NAME
            packaged_smoke.write_json(resume_path, resume)
            packaged_smoke.write_json(server, packaged_smoke.server_verification(REMOTE_NAME, {"customer_id": "other", "customer_name": "Packaged server offline"}))
            with self.assertRaisesRegex(packaged_smoke.EvidenceError, "server verification does not confirm"):
                packaged_smoke.complete_server_cell(REPO_ROOT, packaged_smoke.required_cells(REPO_ROOT)[0], directory / "cell.json", initial_path, resume_path, 101, [artifact], packaged_smoke.hash_files([artifact]), remote, server)



if __name__ == "__main__":
    unittest.main()
