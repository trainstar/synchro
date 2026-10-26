#!/usr/bin/env python3
"""Run structural controls for packaged smoke evidence."""

from __future__ import annotations

import copy
import base64
import hashlib
import hmac
import gzip
import io
import itertools
import json
import os
import re
import socket
import subprocess
import sys
import tempfile
import tarfile
import threading
import unittest
import urllib.error
import urllib.request
from pathlib import Path
from unittest import mock

sys.dont_write_bytecode = True
from verification import packaged_smoke
from verification.test_support_environments import valid_records as selected_environments


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


def measured_environment(cell_id: str) -> dict[str, str]:
    # Independently authored measurement fixtures; never derive these from the manifest.
    return {
        "SUP-PG-LINUX-X64-001": {"architecture": "x86_64", "os": "ubuntu-24.04", "postgresql": "18.3"},
        "SUP-IOS-MIN-001": {"ios": "17.0", "xcode": "27.0"},
        "SUP-IOS-CURRENT-001": {"ios": "27.0", "xcode": "27.0"},
        "SUP-RN-IOS-CURRENT-001": {"ios": "27.0", "xcode": "27.0", "react_native": "0.88.0"},
        "SUP-RN-IOS-MIN-001": {"ios": "27.0", "xcode": "27.0", "react_native": "0.82.1"},
        "SUP-ANDROID-MIN-001": {
            "android_api": "24", "os": "ubuntu-24.04",
            "system_image": "system-images;android-24;google_apis;x86_64", "system_image_revision": "27",
            "emulator_version": "37.2.12", "emulator_build": "16428233",
        },
        "SUP-ANDROID-CURRENT-001": {
            "android_api": "37", "os": "ubuntu-24.04",
            "system_image": "system-images;android-37.0;google_apis;x86_64", "system_image_revision": "6",
            "emulator_version": "37.2.12", "emulator_build": "16428233",
        },
        "SUP-RN-ANDROID-CURRENT-001": {
            "android_api": "37", "os": "ubuntu-24.04",
            "system_image": "system-images;android-37.0;google_apis;x86_64", "system_image_revision": "6",
            "emulator_version": "37.2.12", "emulator_build": "16428233", "react_native": "0.88.0",
        },
        "SUP-RN-ANDROID-MIN-001": {
            "android_api": "37", "os": "ubuntu-24.04",
            "system_image": "system-images;android-37.0;google_apis;x86_64", "system_image_revision": "6",
            "emulator_version": "37.2.12", "emulator_build": "16428233", "react_native": "0.82.1",
        },
    }[cell_id]


class PackagedSmokeStructureTests(unittest.TestCase):
    def test_kotlin_initial_readiness_retains_failed_commands_without_payloads(self) -> None:
        script = (REPO_ROOT / "verification/consumers/kotlin/test-consumer-device.sh").read_text()
        readiness = script.split("ready=0\n", 1)[1].split("\nset +e\n", 1)[0]
        for read_status, pid_status, output, current_pid in (
            (255, 255, "cat: files/initial-result.json: No such file or directory", "private-token"),
            (0, 0, '{"status":"failed","error":"private-token","observed":{"row":"private-row"}}', "202"),
        ):
            with self.subTest(read_status=read_status, pid_status=pid_status), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                (root / "work").mkdir()
                shell = """set -eu
work_dir="$1/work"
environment_dir="$1/evidence"
package=com.trainstar.synchro.consumer
initial_pid=101
ready=0
adb_command() {
  case "$1" in
    exec-out) printf '%s' "$RESULT_OUTPUT"; printf '%s' 'private-token' >&2; return "$READ_STATUS" ;;
    shell) printf '%s\\r\\n' "$CURRENT_PID"; printf '%s' 'error: closed private-token' >&2; return "$PID_STATUS" ;;
    logcat) printf '%s\\n' 'FATAL EXCEPTION: private-token private-row' 'java.lang.IllegalStateException: private-token'; return 0 ;;
    *) exit 99 ;;
  esac
}
""" + readiness
                run = subprocess.run(
                    ["sh", "-s", "--", str(root)], input=shell, text=True, capture_output=True,
                    env={**os.environ, "READ_STATUS": str(read_status), "PID_STATUS": str(pid_status),
                         "RESULT_OUTPUT": output, "CURRENT_PID": current_pid}, timeout=10,
                )
                self.assertEqual(run.returncode, 1, run.stderr)
                evidence = root / "evidence/initial-readiness-failure.json"
                report = json.loads(evidence.read_text())
                self.assertEqual(report["initial_result_returncode"], read_status)
                self.assertEqual(report["liveness_returncode"], pid_status)
                self.assertEqual(report["expected_pid"], 101)
                self.assertEqual(report["observed_pid"], 202 if pid_status == 0 else None)
                self.assertEqual(report["initial_result_bytes"], len(output.encode()))
                self.assertTrue(report["android_runtime_fatal_exception"])
                self.assertEqual(report["exception_classes"], ["java.lang.IllegalStateException"])
                if read_status == 0:
                    self.assertEqual(report["initial_result_status"], "failed")
                for secret in ("private-token", "private-row"):
                    self.assertNotIn(secret, evidence.read_text())
                    self.assertNotIn(secret, run.stdout + run.stderr)
                self.assertLess(evidence.stat().st_size, 2048)

    def consumer_artifacts(self, directory: Path, cell: str) -> list[Path]:
        directory.mkdir(parents=True, exist_ok=True)
        version = (REPO_ROOT / "VERSION").read_text().strip()
        files = []
        if cell.startswith("SUP-RN-"):
            npm = directory / "npm" / f"trainstar-synchro-react-native-{version}.tgz"
            npm.parent.mkdir(parents=True, exist_ok=True)
            npm.write_bytes(b"authored npm fixture")
            files.append(npm)
        if "ANDROID" in cell:
            base = directory / "maven/fit/trainstar/synchro" / version / f"synchro-{version}"
            base.parent.mkdir(parents=True, exist_ok=True)
            aar, pom, module = (Path(str(base) + suffix) for suffix in (".aar", ".pom", ".module"))
            aar.write_bytes(b"authored AAR fixture")
            pom.write_text(f"<project><groupId>fit.trainstar</groupId><artifactId>synchro</artifactId><version>{version}</version><packaging>aar</packaging></project>")
            packaged_smoke.write_json(module, {"formatVersion": "1.1",
                "component": {"group": "fit.trainstar", "module": "synchro", "version": version},
                "variants": [{"name": "releaseRuntimeElements", "attributes": {"org.gradle.category": "library"},
                              "files": [{"name": aar.name, "url": aar.name}]}]})
            files.extend((aar, pom, module))
        if "IOS" in cell:
            data = packaged_smoke.release_artifacts.git_output("-C", str(REPO_ROOT), "archive", "--format=tar", "--prefix=Synchro/", "HEAD", *packaged_smoke.release_artifacts.APPLE_SOURCE_PATHS)
            apple = directory / "apple"
            apple.mkdir()
            archive_path = apple / f"synchro-spm-{version}.tar.gz"
            archive_path.write_bytes(gzip.compress(data, mtime=0))
            with tarfile.open(fileobj=io.BytesIO(data), mode="r:") as archive:
                for member in archive.getmembers():
                    if member.isfile():
                        path = apple / member.name
                        path.parent.mkdir(parents=True, exist_ok=True)
                        path.write_bytes(archive.extractfile(member).read())
            files.append(archive_path)
        return files

    def local_inputs(self, directory: Path, arguments: tuple) -> Path:
        environments = directory / "expected-environments.json"
        output = directory / "local-inputs.json"
        packaged_smoke.write_json(environments, selected_environments())
        if arguments[1] != "SUP-PG-LINUX-X64-001":
            arguments[6][:] = self.consumer_artifacts(directory / "consumer", arguments[1])
            arguments[7][:] = packaged_smoke.hash_files(arguments[6])
        if arguments[1].startswith("SUP-RN-IOS-"):
            subprocess.run(["git", "clone", "--bare", "--quiet", str(REPO_ROOT), str(directory / "source.git")], check=True)
            subprocess.run(["git", "--git-dir", str(directory / "source.git"), "tag", "-f", "v" + (REPO_ROOT / "VERSION").read_text().strip(), packaged_smoke.source_commit(REPO_ROOT)], check=True, capture_output=True)
        packaged_smoke.prepare_local_cell(REPO_ROOT, arguments[1], environments, arguments[6], output,
                                          artifact_dir=directory / "consumer")
        return output

    def local_completion_options(self, arguments: tuple) -> dict:
        if arguments[1] == "SUP-PG-LINUX-X64-001":
            return {}
        options = {"artifact_dir": arguments[2].parent / "consumer"}
        if arguments[1].startswith("SUP-RN-IOS-"):
            options["git_dir"] = arguments[2].parent / "source.git"
        return options

    def test_prepared_a_cannot_certify_consumed_b_for_each_client_path(self) -> None:
        for cell in ("SUP-IOS-CURRENT-001", "SUP-ANDROID-MIN-001", "SUP-RN-ANDROID-MIN-001", "SUP-RN-IOS-CURRENT-001"):
            with self.subTest(cell=cell), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                arguments = self.completion_arguments(root, cell)
                inputs = self.local_inputs(root, arguments)
                other = root / "other-consumer"
                other_files = self.consumer_artifacts(other, cell)
                version = (REPO_ROOT / "VERSION").read_text().strip()
                if cell.startswith("SUP-RN-"):
                    command = ["sh", str(REPO_ROOT / "verification/consumers/react-native/test-consumer.sh"), "android" if "ANDROID" in cell else "ios", str(other), version]
                elif "ANDROID" in cell:
                    command = ["sh", str(REPO_ROOT / "verification/consumers/kotlin/test-consumer-device.sh"), str(REPO_ROOT), str(other), cell, str(arguments[2]), version]
                else:
                    command = ["sh", str(REPO_ROOT / "verification/consumers/swift-ios/test-consumer.sh"), str(other)]
                blocked = root / "blocked-commands"
                blocked.mkdir()
                calls = root / "native-command-called"
                for name in ("xcodebuild", "xcrun", "npx", "npm", "adb"):
                    executable = blocked / name
                    executable.write_text(f'#!/bin/sh\nprintf called > "{calls}"\nexit 99\n')
                    executable.chmod(0o700)
                environment = {**os.environ, "PACKAGED_SMOKE_CELL_ID": cell, "PACKAGED_SMOKE_CELL_RESULT": str(arguments[2]),
                               "PACKAGED_SMOKE_LOCAL_INPUTS": str(inputs), "PACKAGED_SMOKE_RELEASE_MANIFEST": "",
                               "PACKAGED_SMOKE_DISTRIBUTION_ARTIFACTS": " ".join(map(str, arguments[6])),
                               "PACKAGED_SMOKE_EXPECTED_ARTIFACT_HASHES": " ".join(arguments[7]),
                               "IOS_SIMULATOR_UDID": "must-not-reach-device", "SYNCHRO_CONSUMER_RESOLUTION": "prepublication",
                               "SYNCHRO_PREPUBLICATION_GIT_URL": "", "PATH": str(blocked) + os.pathsep + os.environ["PATH"]}
                result = subprocess.run(command, env=environment, capture_output=True, text=True)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("selected consumer files", result.stderr)
                self.assertFalse(calls.exists())
                self.assertFalse(arguments[2].exists())
                options = self.local_completion_options(arguments)
                options["artifact_dir"] = other
                with self.assertRaisesRegex(packaged_smoke.EvidenceError, "selected consumer files"):
                    packaged_smoke.complete_cell(*arguments[:-1], None, local_inputs=inputs, **options)
                self.assertFalse(arguments[2].exists())
                packaged_smoke.complete_cell(*arguments[:-1], None, local_inputs=inputs, **self.local_completion_options(arguments))
                with self.assertRaises(packaged_smoke.EvidenceError):
                    packaged_smoke.verify_local_cell(REPO_ROOT, cell, inputs, arguments[2], arguments[6], arguments[7], artifact_dir=other)
                if "ANDROID" in cell:
                    aar = next(path for path in other_files if path.suffix == ".aar")
                    aar.write_bytes(b"different consumed SDK")
                    with self.assertRaisesRegex(packaged_smoke.EvidenceError, "prepared inputs"):
                        packaged_smoke.validate_local_consumer_inputs(REPO_ROOT, cell, other_files, other, inputs=inputs)
                with self.assertRaises(packaged_smoke.EvidenceError):
                    packaged_smoke.prepare_local_cell(REPO_ROOT, cell, root / "expected-environments.json", arguments[6], root / "bad-inputs.json")

    def test_consumed_maven_metadata_changes_fail_before_execution_and_completion(self) -> None:
        for suffix in (".pom", ".module"):
            with self.subTest(suffix=suffix), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                arguments = self.completion_arguments(root, "SUP-ANDROID-MIN-001")
                inputs = self.local_inputs(root, arguments)
                metadata = next(path for path in arguments[6] if path.suffix == suffix)
                if suffix == ".pom":
                    metadata.write_text(metadata.read_text().replace("</project>", "<description>changed metadata</description></project>"))
                else:
                    value = packaged_smoke.load_json(metadata, "module")
                    value["variants"][0]["dependencies"] = [{"group": "example.foreign", "module": "changed", "version": {"requires": "1.0"}}]
                    packaged_smoke.write_json(metadata, value)
                with self.assertRaisesRegex(packaged_smoke.EvidenceError, "prepared inputs"):
                    packaged_smoke.validate_local_consumer_inputs(REPO_ROOT, arguments[1], arguments[6], root / "consumer", inputs=inputs)
                with self.assertRaises(packaged_smoke.EvidenceError):
                    packaged_smoke.complete_cell(*arguments[:-1], None, local_inputs=inputs, **self.local_completion_options(arguments))
                self.assertFalse(arguments[2].exists())

    def test_version_specific_manifest_rejects_unchanged_archive_before_native_completion_and_verification(self) -> None:
        cells = ("SUP-IOS-CURRENT-001", "SUP-RN-IOS-CURRENT-001")
        manifests = ("Package@swift-6.0.swift", "package@swift-6.swift", "PACKAGE@SWIFT-6.SWIFT")
        for cell, manifest in itertools.product(cells, manifests):
            with self.subTest(cell=cell, manifest=manifest), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                arguments = self.completion_arguments(root, cell)
                inputs = self.local_inputs(root, arguments)
                original_inputs = inputs.read_bytes()
                self.assertEqual(len(arguments[6]), 2 if cell.startswith("SUP-RN-") else 1)
                options = self.local_completion_options(arguments)
                packaged_smoke.complete_cell(*arguments[:-1], None, local_inputs=inputs, **options)
                retained = root / "retained-cell.json"
                retained.write_bytes(arguments[2].read_bytes())
                arguments[2].unlink()
                packaged_smoke.verify_local_cell(REPO_ROOT, cell, inputs, retained, arguments[6], arguments[7], artifact_dir=root / "consumer")

                package = root / "consumer/apple/Synchro"
                (package / manifest).write_text(
                    '// swift-tools-version: 6.0\n'
                    'import PackageDescription\n'
                    'let package = Package(name: "Synchro", products: [.library(name: "Synchro", targets: ["Synchro"])], '
                    'targets: [.target(name: "Synchro", path: "AlternateSources")])\n'
                )
                alternate = package / "AlternateSources"
                alternate.mkdir()
                (alternate / "Foreign.swift").write_text('public enum ForeignImplementation {}\n')
                self.assertEqual(inputs.read_bytes(), original_inputs)
                self.assertEqual(packaged_smoke.hash_files(arguments[6]), arguments[7])
                rejected_preparation = root / "rejected-inputs.json"
                with self.assertRaisesRegex(packaged_smoke.EvidenceError, "version-specific Swift manifest"):
                    packaged_smoke.prepare_local_cell(REPO_ROOT, cell, root / "expected-environments.json",
                                                      arguments[6], rejected_preparation, artifact_dir=root / "consumer")
                self.assertFalse(rejected_preparation.exists())

                blocked = root / "blocked-commands"
                blocked.mkdir()
                calls = root / "native-command-called"
                for name in ("xcodebuild", "xcrun", "npx", "npm", "adb"):
                    executable = blocked / name
                    executable.write_text(f'#!/bin/sh\nprintf called > "{calls}"\nexit 99\n')
                    executable.chmod(0o700)
                environment = {**os.environ, "PACKAGED_SMOKE_CELL_ID": cell, "PACKAGED_SMOKE_CELL_RESULT": str(arguments[2]),
                               "PACKAGED_SMOKE_LOCAL_INPUTS": str(inputs), "PACKAGED_SMOKE_RELEASE_MANIFEST": "",
                               "PACKAGED_SMOKE_DISTRIBUTION_ARTIFACTS": " ".join(map(str, arguments[6])),
                               "PACKAGED_SMOKE_EXPECTED_ARTIFACT_HASHES": " ".join(arguments[7]),
                               "IOS_SIMULATOR_UDID": "must-not-reach-device", "SYNCHRO_CONSUMER_RESOLUTION": "prepublication",
                               "SYNCHRO_PREPUBLICATION_GIT_URL": "", "PATH": str(blocked) + os.pathsep + os.environ["PATH"]}
                if cell.startswith("SUP-RN-"):
                    command = ["sh", str(REPO_ROOT / "verification/consumers/react-native/test-consumer.sh"), "ios", str(root / "consumer"), (REPO_ROOT / "VERSION").read_text().strip()]
                else:
                    command = ["sh", str(REPO_ROOT / "verification/consumers/swift-ios/test-consumer.sh"), str(root / "consumer")]
                result = subprocess.run(command, env=environment, capture_output=True, text=True)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("version-specific Swift manifest", result.stderr)
                self.assertFalse(calls.exists())
                self.assertFalse(arguments[2].exists())
                with self.assertRaisesRegex(packaged_smoke.EvidenceError, "version-specific Swift manifest"):
                    packaged_smoke.complete_cell(*arguments[:-1], None, local_inputs=inputs, **options)
                self.assertFalse(arguments[2].exists())
                with self.assertRaisesRegex(packaged_smoke.EvidenceError, "version-specific Swift manifest"):
                    packaged_smoke.verify_local_cell(REPO_ROOT, cell, inputs, retained, arguments[6], arguments[7], artifact_dir=root / "consumer")

    def test_expanded_apple_changes_and_wrong_rn_git_resolution_fail_closed(self) -> None:
        for mutation in ("expanded", "extra-source", "archive", "public", "external-git", "wrong-tag", "foreign-origin"):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                arguments = self.completion_arguments(root, "SUP-RN-IOS-CURRENT-001")
                inputs = self.local_inputs(root, arguments)
                if mutation == "expanded":
                    (root / "consumer/apple/Synchro/Package.swift").write_text("foreign package sources")
                elif mutation == "extra-source":
                    (root / "consumer/apple/Synchro/clients/swift/Sources/Foreign.swift").write_text("foreign source")
                elif mutation == "archive":
                    next(path for path in arguments[6] if path.name.endswith(".tar.gz")).write_bytes(b"foreign archive")
                elif mutation == "wrong-tag":
                    subprocess.run(["git", "--git-dir", str(root / "source.git"), "tag", "-f", "v" + (REPO_ROOT / "VERSION").read_text().strip(), "HEAD~1"], check=True, capture_output=True)
                elif mutation == "foreign-origin":
                    subprocess.run(["git", "--git-dir", str(root / "source.git"), "config", "remote.origin.url", str(root / "foreign.git")], check=True)
                inherited = {"SYNCHRO_CONSUMER_RESOLUTION": "public"} if mutation == "public" else {"SYNCHRO_PREPUBLICATION_GIT_URL": "file:///foreign.git"} if mutation == "external-git" else {}
                with mock.patch.dict(os.environ, inherited), self.assertRaises(packaged_smoke.EvidenceError):
                    packaged_smoke.complete_cell(*arguments[:-1], None, local_inputs=inputs, **self.local_completion_options(arguments))
                self.assertFalse(arguments[2].exists())

    def test_local_client_and_server_complete_same_lifecycle_but_not_sealed_receipts(self) -> None:
        for cell_id in ("SUP-PG-LINUX-X64-001", "SUP-RN-IOS-CURRENT-001"):
            with self.subTest(cell=cell_id), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                arguments = self.completion_arguments(root, cell_id)
                inputs = self.local_inputs(root, arguments)
                complete = packaged_smoke.complete_server_cell if cell_id == "SUP-PG-LINUX-X64-001" else packaged_smoke.complete_cell
                complete(*arguments[:-1], None, local_inputs=inputs, **self.local_completion_options(arguments))
                cell = packaged_smoke.load_json(arguments[2], "local cell")
                self.assertEqual(cell["evidence_kind"], "local-source-artifacts")
                self.assertEqual(cell["environment"], measured_environment(cell_id))
                self.assertEqual(cell["process_lifecycle"]["kill_signal"], 9)
                self.assertEqual(cell["lifecycle"], {"status": "passed", "terminal": True, "test_count": 1})
                packaged_smoke.verify_local_cell(REPO_ROOT, cell_id, inputs, arguments[2], arguments[6], arguments[7], artifact_dir=root / "consumer")
                with self.assertRaises(packaged_smoke.EvidenceError):
                    packaged_smoke.validate_cell(cell, cell_id, packaged_smoke.source_commit(REPO_ROOT))
                cells = root / "cells"
                packaged_smoke.write_json(cells / f"{cell_id}.json", cell)
                with self.assertRaises(packaged_smoke.EvidenceError):
                    packaged_smoke.collect_summary(REPO_ROOT, cells, root / "sealed-summary.json")

    def test_local_input_closed_schema_source_cell_environments_and_hashes_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            arguments = self.completion_arguments(root, "SUP-IOS-CURRENT-001")
            inputs = self.local_inputs(root, arguments)
            original = packaged_smoke.load_json(inputs, "local inputs")
            self.assertEqual(set(original), {"schema_version", "kind", "source_commit", "cell_id",
                                             "resolved_support_cells", "artifact_hashes"})
            for mutation in ("extra", "missing", "schema", "boolean-schema", "kind", "source", "malformed-source",
                             "cell", "missing-cell", "duplicate-cell", "unknown-environment-field",
                             "empty-hashes", "duplicate-hashes", "zero-hash", "malformed-hash"):
                bad = copy.deepcopy(original)
                if mutation == "extra":
                    bad["candidate_ci_run"] = 1
                elif mutation == "missing":
                    del bad["kind"]
                elif mutation in {"schema", "boolean-schema"}:
                    bad["schema_version"] = 2 if mutation == "schema" else True
                elif mutation == "kind":
                    bad["kind"] = "release-manifest"
                elif mutation in {"source", "malformed-source"}:
                    bad["source_commit"] = "a" * 40 if mutation == "source" else "HEAD"
                elif mutation == "cell":
                    bad["cell_id"] = "SUP-IOS-MIN-001"
                elif mutation == "missing-cell":
                    bad["resolved_support_cells"].pop()
                elif mutation == "duplicate-cell":
                    bad["resolved_support_cells"].append(copy.deepcopy(bad["resolved_support_cells"][0]))
                elif mutation == "unknown-environment-field":
                    bad["resolved_support_cells"][0]["environment"]["unknown"] = "1"
                elif mutation == "empty-hashes":
                    bad["artifact_hashes"] = []
                elif mutation == "duplicate-hashes":
                    bad["artifact_hashes"] *= 2
                else:
                    bad["artifact_hashes"] = ["0" * 64 if mutation == "zero-hash" else "not-a-hash"]
                packaged_smoke.write_json(inputs, bad)
                with self.subTest(mutation=mutation), self.assertRaises(packaged_smoke.EvidenceError):
                    packaged_smoke.complete_cell(*arguments[:-1], None, local_inputs=inputs, **self.local_completion_options(arguments))
                self.assertFalse(arguments[2].exists())
            with mock.patch.object(packaged_smoke.subprocess, "check_output", side_effect=[
                packaged_smoke.source_commit(REPO_ROOT) + "\n", " M source.py\n",
            ]), self.assertRaises(packaged_smoke.EvidenceError):
                packaged_smoke.clean_source_commit(REPO_ROOT)

    def test_local_preparation_requires_complete_resolution_and_actual_nonempty_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            environments = root / "support.json"
            packaged_smoke.write_json(environments, selected_environments())
            artifact = root / "artifact"
            artifact.write_bytes(b"actual source-built fixture bytes")
            output = root / "local.json"
            packaged_smoke.prepare_local_cell(REPO_ROOT, "SUP-PG-LINUX-X64-001", environments, [artifact], output)
            value = packaged_smoke.load_json(output, "local inputs")
            self.assertEqual(value["artifact_hashes"], [hashlib.sha256(artifact.read_bytes()).hexdigest()])
            output.unlink()
            for files in ([], [root / "missing"], [root]):
                with self.subTest(files=files), self.assertRaises(packaged_smoke.EvidenceError):
                    packaged_smoke.prepare_local_cell(REPO_ROOT, "SUP-PG-LINUX-X64-001", environments, files, output)
            artifact.write_bytes(b"")
            with self.assertRaises(packaged_smoke.EvidenceError):
                packaged_smoke.prepare_local_cell(REPO_ROOT, "SUP-PG-LINUX-X64-001", environments, [artifact], output)
            packaged_smoke.write_json(environments, selected_environments()[:-1])
            with self.assertRaises(packaged_smoke.EvidenceError):
                packaged_smoke.prepare_local_cell(REPO_ROOT, "SUP-PG-LINUX-X64-001", environments, [artifact], output)
            self.assertFalse(output.exists())

    def test_local_completion_and_verification_reject_environment_hash_and_lifecycle_changes(self) -> None:
        for cell_id in ("SUP-PG-LINUX-X64-001", "SUP-IOS-CURRENT-001"):
            with self.subTest(cell=cell_id), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                arguments = self.completion_arguments(root, cell_id)
                inputs = self.local_inputs(root, arguments)
                complete = packaged_smoke.complete_server_cell if cell_id == "SUP-PG-LINUX-X64-001" else packaged_smoke.complete_cell
                complete(*arguments[:-1], None, local_inputs=inputs, **self.local_completion_options(arguments))
                original = packaged_smoke.load_json(arguments[2], "local cell")
                for mutation in ("environment", "hash", "zero-lifecycle", "nonterminal", "source", "cell", "kind", "failure"):
                    bad = copy.deepcopy(original)
                    if mutation == "environment":
                        field = "postgresql" if cell_id == "SUP-PG-LINUX-X64-001" else "ios"
                        bad["environment"][field] = "18.4" if field == "postgresql" else "27.0.1"
                    elif mutation == "hash":
                        bad["artifact_hashes"] = ["f" * 64]
                    elif mutation == "zero-lifecycle":
                        bad["lifecycle"]["test_count"] = 0
                    elif mutation == "nonterminal":
                        bad["lifecycle"]["terminal"] = False
                    elif mutation == "source":
                        bad["source_commit"] = "a" * 40
                    elif mutation == "cell":
                        bad["cell_id"] = "SUP-IOS-MIN-001"
                    elif mutation == "kind":
                        bad["evidence_kind"] = "sealed"
                    else:
                        bad["status"] = "failed"
                        bad["failure"] = "consumer failed"
                        del bad["process_lifecycle"]
                        bad["lifecycle"] = {"status": "failed", "terminal": True, "test_count": 0}
                    packaged_smoke.write_json(arguments[2], bad)
                    with self.subTest(mutation=mutation), self.assertRaises(packaged_smoke.EvidenceError):
                        packaged_smoke.verify_local_cell(REPO_ROOT, cell_id, inputs, arguments[2], arguments[6], arguments[7], artifact_dir=root / "consumer")
                packaged_smoke.write_json(arguments[2], original)
                with self.assertRaises(packaged_smoke.EvidenceError):
                    packaged_smoke.verify_local_cell(REPO_ROOT, cell_id, inputs, arguments[2], arguments[6], ["f" * 64], artifact_dir=root / "consumer")
                arguments[6][0].write_bytes(b"changed artifact bytes after preparation")
                with self.assertRaises(packaged_smoke.EvidenceError):
                    packaged_smoke.verify_local_cell(REPO_ROOT, cell_id, inputs, arguments[2], arguments[6], arguments[7], artifact_dir=root / "consumer")
                with self.assertRaises(packaged_smoke.EvidenceError):
                    complete(*arguments[:-1], None, local_inputs=inputs, **self.local_completion_options(arguments))

    def test_local_completion_rejects_changed_independent_measurements_and_input_hash(self) -> None:
        for change in ("initial", "both", "input-hash"):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                arguments = self.completion_arguments(root, "SUP-IOS-CURRENT-001")
                inputs = self.local_inputs(root, arguments)
                if change == "input-hash":
                    value = packaged_smoke.load_json(inputs, "inputs")
                    value["artifact_hashes"] = ["f" * 64]
                    packaged_smoke.write_json(inputs, value)
                else:
                    for path in arguments[10:12] if change == "both" else arguments[10:11]:
                        value = packaged_smoke.load_json(path, "measurement")
                        value["environment"]["ios"] = "27.0.1"
                        packaged_smoke.write_json(path, value)
                with self.assertRaises(packaged_smoke.EvidenceError):
                    packaged_smoke.complete_cell(*arguments[:-1], None, local_inputs=inputs, **self.local_completion_options(arguments))
                self.assertFalse(arguments[2].exists())

    def test_cli_and_consumer_boundaries_reject_missing_or_both_explicit_inputs_before_execution(self) -> None:
        scripts = REPO_ROOT / "verification/consumers"
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            record = root / "input.json"
            record.write_text("{}")
            commands = [
                ["sh", str(scripts / "swift-ios/test-consumer.sh"), str(root)],
                ["sh", str(scripts / "kotlin/test-consumer-device.sh"), str(REPO_ROOT), str(root), "SUP-ANDROID-MIN-001", str(root / "cell.json"), "0.4.0-rc.2"],
                ["sh", str(scripts / "react-native/test-consumer.sh"), "ios", str(root), "0.4.0-rc.2"],
                ["sh", str(scripts / "server/test-consumer.sh"), *([str(root)] * 10)],
            ]
            for both in (False, True):
                environment = {**os.environ, "PACKAGED_SMOKE_CELL_ID": "SUP-IOS-MIN-001",
                               "PACKAGED_SMOKE_CELL_RESULT": str(root / "cell.json"),
                               "PACKAGED_SMOKE_RELEASE_MANIFEST": str(record) if both else "",
                               "PACKAGED_SMOKE_LOCAL_INPUTS": str(record) if both else ""}
                for command in commands:
                    with self.subTest(command=command[1], both=both):
                        result = subprocess.run(command, env=environment, capture_output=True, text=True)
                        self.assertNotEqual(result.returncode, 0)
                        self.assertIn("exactly one", result.stderr)
                for command in ("complete-cell", "complete-server-cell"):
                    args = [sys.executable, str(REPO_ROOT / "verification/packaged_smoke.py"), command,
                            "--cell", "SUP-IOS-MIN-001", "--killed-pid", "101"]
                    for flag in ("--repo-root", "--output", "--initial", "--resume", "--remote", "--server-verification",
                                 "--initial-environment", "--resume-environment"):
                        args.extend([flag, str(root)])
                    if both:
                        args.extend(["--release-manifest", str(record), "--local-inputs", str(record)])
                    result = subprocess.run(args, capture_output=True, text=True)
                    self.assertEqual(result.returncode, 2)

    def test_all_completed_profiles_retain_independent_measurements(self) -> None:
        for cell_id in packaged_smoke.required_cells(REPO_ROOT):
            with self.subTest(cell=cell_id), tempfile.TemporaryDirectory() as directory:
                arguments = self.completion_arguments(Path(directory), cell_id)
                self.complete_fixture(arguments)
                cell = packaged_smoke.load_json(arguments[2], "completed cell")
                self.assertEqual(cell["environment"], measured_environment(cell_id))
                validated = packaged_smoke.validate_cell(cell, cell_id, packaged_smoke.source_commit(REPO_ROOT))
                self.assertEqual(validated["environment"], measured_environment(cell_id))
                self.assertEqual(cell["lifecycle"], {"status": "passed", "terminal": True, "test_count": 1})

    def test_changed_initial_resume_or_selected_environment_rejects_completion(self) -> None:
        for cell_id, changes in (
            ("SUP-PG-LINUX-X64-001", {"postgresql": "18.4"}),
            ("SUP-IOS-MIN-001", {"ios": "17.1"}),
            ("SUP-IOS-CURRENT-001", {"ios": "27.0.1"}),
            ("SUP-IOS-CURRENT-001", {"xcode": "27.0.1"}),
            ("SUP-ANDROID-CURRENT-001", {"android_api": "38", "system_image": "system-images;android-38;google_apis;x86_64"}),
            ("SUP-ANDROID-CURRENT-001", {"system_image_revision": "7"}),
            ("SUP-ANDROID-CURRENT-001", {"system_image": "system-images;android-37;google_apis;x86_64"}),
            ("SUP-ANDROID-CURRENT-001", {"emulator_version": "37.2.13"}),
            ("SUP-ANDROID-CURRENT-001", {"emulator_build": "16428234"}),
            ("SUP-RN-IOS-CURRENT-001", {"react_native": "0.88.1"}),
            ("SUP-RN-ANDROID-CURRENT-001", {"react_native": "0.88.1"}),
        ):
            for phase in ("initial", "resume", "both"):
                with self.subTest(cell=cell_id, changes=changes, phase=phase), tempfile.TemporaryDirectory() as directory:
                    arguments = self.completion_arguments(Path(directory), cell_id)
                    paths = arguments[10:12] if phase == "both" else [arguments[10 if phase == "initial" else 11]]
                    for path in paths:
                        record = packaged_smoke.load_json(path, "measurement")
                        record["environment"].update(changes)
                        packaged_smoke.write_json(path, record)
                    message = "does not match sealed" if phase == "both" else "initial and resume measured environments differ"
                    with self.assertRaisesRegex(packaged_smoke.EvidenceError, message):
                        self.complete_fixture(arguments)
                    self.assertFalse(arguments[2].exists())

    def test_completion_rejects_wrong_ids_source_and_incomplete_or_duplicate_input(self) -> None:
        for cell_id in ("SUP-PG-LINUX-X64-001", "SUP-IOS-CURRENT-001"):
            for mutation in ("initial-id", "resume-id", "source", "duplicate-cell", "missing-cell",
                             "duplicate-member", "missing-field", "missing-file", "record-array"):
                with self.subTest(cell=cell_id, mutation=mutation), tempfile.TemporaryDirectory() as directory:
                    arguments = self.completion_arguments(Path(directory), cell_id)
                    initial, resume, manifest_path = arguments[10:13]
                    if mutation in {"initial-id", "resume-id"}:
                        packaged_smoke.write_json(initial if mutation == "initial-id" else resume, {
                            "id": "SUP-IOS-MIN-001", "environment": measured_environment("SUP-IOS-MIN-001"),
                        })
                    elif mutation == "duplicate-member":
                        text = initial.read_text()
                        field = "postgresql" if cell_id == "SUP-PG-LINUX-X64-001" else "ios"
                        actual = measured_environment(cell_id)[field]
                        initial.write_text(text.replace(f'"{field}": "{actual}"', f'"{field}": "{actual}", "{field}": "{actual}"', 1))
                    elif mutation == "missing-field":
                        record = packaged_smoke.load_json(initial, "measurement")
                        record["environment"].pop(next(iter(record["environment"])))
                        packaged_smoke.write_json(initial, record)
                    elif mutation == "missing-file":
                        initial.unlink()
                    elif mutation == "record-array":
                        packaged_smoke.write_json(initial, [packaged_smoke.load_json(initial, "measurement")])
                    else:
                        manifest = packaged_smoke.load_json(manifest_path, "manifest")
                        if mutation == "source":
                            manifest["source"]["commit"] = "a" * 40
                        elif mutation == "duplicate-cell":
                            manifest["resolved_support_cells"].append(copy.deepcopy(manifest["resolved_support_cells"][0]))
                        else:
                            manifest["resolved_support_cells"].pop()
                        packaged_smoke.write_json(manifest_path, manifest)
                    with self.assertRaises(packaged_smoke.EvidenceError):
                        self.complete_fixture(arguments)
                    self.assertFalse(arguments[2].exists())

    def test_passed_cells_reject_missing_malformed_and_unknown_environments(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            arguments = self.completion_arguments(Path(directory), "SUP-IOS-CURRENT-001")
            self.complete_fixture(arguments)
            cell = packaged_smoke.load_json(arguments[2], "cell")
            for mutation in ("missing", "empty", "nonstring", "unknown-field", "unknown-cell"):
                bad = copy.deepcopy(cell)
                expected = bad["cell_id"]
                if mutation == "missing":
                    del bad["environment"]
                elif mutation == "empty":
                    bad["environment"] = {}
                elif mutation == "nonstring":
                    bad["environment"]["ios"] = 27
                elif mutation == "unknown-field":
                    bad["environment"]["unknown"] = "27.0"
                else:
                    expected = bad["cell_id"] = "SUP-MACOS-CURRENT-001"
                with self.subTest(mutation=mutation), self.assertRaises(packaged_smoke.EvidenceError):
                    packaged_smoke.validate_cell(bad, expected, packaged_smoke.source_commit(REPO_ROOT))

    def test_complete_summary_preserves_nine_measurements_and_lifecycles(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            _, output = self.completed_summary(Path(directory))
            summary = packaged_smoke.load_json(output, "summary")
            self.assertEqual(summary["status"], "passed")
            self.assertEqual(summary["resolved_support_cells"], [
                {"id": cell_id, "environment": measured_environment(cell_id)}
                for cell_id in sorted(packaged_smoke.required_cells(REPO_ROOT))
            ])
            self.assertEqual(len(summary["cells"]), 9)
            self.assertTrue(all(cell["status"] == "passed" and cell["terminal"] and cell["test_count"] == 1 for cell in summary["cells"]))
            self.assertNotIn("missing_environment_cells", summary)
            packaged_smoke.verify_summary(REPO_ROOT, output)

    def test_legacy_cell_requires_all_terminal_operations_before_normalization(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            arguments = self.completion_arguments(Path(directory), "SUP-IOS-CURRENT-001")
            self.complete_fixture(arguments)
            legacy = packaged_smoke.load_json(arguments[2], "completed cell")
            legacy["schema_version"] = 1
            del legacy["lifecycle"]
            legacy["operations"] = [
                {"name": name, "status": "passed", "terminal": True, "test_count": 1}
                for name in packaged_smoke.LEGACY_SMOKE_OPERATIONS
            ]
            normalized = packaged_smoke.validate_cell(legacy, arguments[1], packaged_smoke.source_commit(REPO_ROOT))
            self.assertEqual(normalized["lifecycle"], {"status": "passed", "terminal": True, "test_count": 1})
            for mutation in ("missing", "duplicate", "zero", "boolean-count", "failed", "nonterminal", "bad-process"):
                bad = copy.deepcopy(legacy)
                if mutation == "missing":
                    bad["operations"].pop()
                elif mutation == "duplicate":
                    bad["operations"][1]["name"] = bad["operations"][0]["name"]
                elif mutation == "zero":
                    bad["operations"][2]["test_count"] = 0
                elif mutation == "boolean-count":
                    bad["operations"][2]["test_count"] = True
                elif mutation == "failed":
                    bad["operations"][3]["status"] = "failed"
                elif mutation == "nonterminal":
                    bad["operations"][4]["terminal"] = False
                else:
                    bad["process_lifecycle"]["durable_pending_before_kill"] = 0
                with self.subTest(mutation=mutation), self.assertRaises(packaged_smoke.EvidenceError):
                    packaged_smoke.validate_cell(bad, arguments[1], packaged_smoke.source_commit(REPO_ROOT))

    def test_summary_rejects_missing_duplicate_unknown_and_nonterminal_cells(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            _, output = self.completed_summary(Path(directory))
            original = packaged_smoke.load_json(output, "summary")
            for mutation in ("missing", "duplicate", "unknown", "zero", "boolean-count", "failed", "nonterminal", "foreign-hash"):
                bad = copy.deepcopy(original)
                if mutation == "missing":
                    bad["cells"].pop()
                elif mutation == "duplicate":
                    bad["cells"].append(copy.deepcopy(bad["cells"][0]))
                elif mutation == "unknown":
                    bad["cells"][0]["id"] = "SUP-UNKNOWN-001"
                elif mutation == "zero":
                    bad["cells"][0]["test_count"] = 0
                elif mutation == "boolean-count":
                    bad["cells"][0]["test_count"] = True
                elif mutation == "failed":
                    bad["cells"][0]["status"] = "failed"
                elif mutation == "nonterminal":
                    bad["cells"][0]["terminal"] = False
                else:
                    bad["cells"][0]["artifact_hashes"] = ["f" * 64]
                packaged_smoke.write_json(output, bad)
                with self.subTest(mutation=mutation), self.assertRaises(packaged_smoke.EvidenceError):
                    packaged_smoke.verify_summary(REPO_ROOT, output)

    def test_passed_summaries_reject_invalid_environment_collections(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            _, output = self.completed_summary(Path(directory))
            original = packaged_smoke.load_json(output, "summary")
            for mutation in ("missing-member", "missing-cell", "duplicate", "unknown", "excluded", "tested", "missing-field",
                             "unknown-field", "wrong-type", "unresolved", "nonrecord", "current-mismatch"):
                bad = copy.deepcopy(original)
                records = bad["resolved_support_cells"]
                if mutation == "missing-member":
                    del bad["resolved_support_cells"]
                elif mutation == "missing-cell":
                    records.pop()
                elif mutation == "duplicate":
                    records.append(copy.deepcopy(records[0]))
                elif mutation in {"unknown", "excluded", "tested"}:
                    records[0]["id"] = {"unknown": "SUP-UNKNOWN", "excluded": "SUP-PG-014", "tested": "SUP-MACOS-CURRENT-001"}[mutation]
                elif mutation == "missing-field":
                    records[0]["environment"].pop("os")
                elif mutation == "unknown-field":
                    records[0]["environment"]["unknown"] = "1"
                elif mutation == "wrong-type":
                    records[0]["environment"]["android_api"] = 37
                elif mutation == "unresolved":
                    records[0]["environment"]["android_api"] = "current"
                elif mutation == "nonrecord":
                    records[0] = None
                else:
                    next(record for record in records if record["id"] == "SUP-IOS-CURRENT-001")["environment"]["ios"] = "27.0.1"
                packaged_smoke.write_json(output, bad)
                with self.subTest(mutation=mutation), self.assertRaises(packaged_smoke.EvidenceError):
                    packaged_smoke.verify_summary(REPO_ROOT, output)

    def test_failed_and_missing_cells_preserve_only_available_measurements(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            cells_dir = root / "cells"
            pg_cell, ios_cell = "SUP-PG-LINUX-X64-001", "SUP-IOS-MIN-001"
            for cell_id in (pg_cell, ios_cell):
                packaged_smoke.begin_cell(REPO_ROOT, cell_id, cells_dir / f"{cell_id}.json")
            measured_path = cells_dir / f"{pg_cell}.json"
            failed = packaged_smoke.load_json(measured_path, "failed cell")
            failed["environment"] = measured_environment(pg_cell)
            packaged_smoke.write_json(measured_path, failed)
            output = root / "summary.json"
            packaged_smoke.collect_summary(REPO_ROOT, cells_dir, output)
            summary = packaged_smoke.load_json(output, "failed summary")
            self.assertEqual(summary["status"], "failed")
            self.assertEqual(summary["resolved_support_cells"], [{"id": pg_cell, "environment": measured_environment(pg_cell)}])
            self.assertEqual(summary["missing_environment_cells"], sorted(set(packaged_smoke.required_cells(REPO_ROOT)) - {pg_cell}))
            self.assertEqual(len(summary["cells"]), 9)
            self.assertTrue(all(record["status"] == "failed" and record["test_count"] == 0 for record in summary["cells"]))
            self.assertNotIn("environment", packaged_smoke.load_json(cells_dir / f"{ios_cell}.json", "begin cell"))
            with self.assertRaisesRegex(packaged_smoke.EvidenceError, "did not pass"):
                packaged_smoke.verify_summary(REPO_ROOT, output)
            failed["environment"]["unknown"] = "1"
            packaged_smoke.write_json(measured_path, failed)
            with self.assertRaises(packaged_smoke.EvidenceError):
                packaged_smoke.collect_summary(REPO_ROOT, cells_dir, root / "bad-summary.json")
            self.assertFalse((root / "bad-summary.json").exists())

    def test_load_json_rejects_duplicate_measurement_cell_summary_and_manifest_members(self) -> None:
        for text in (
            '{"id":"SUP-IOS-MIN-001","environment":{"ios":"16.4","ios":"16.4","xcode":"16.4"}}',
            '{"cell_id":"SUP-IOS-MIN-001","environment":{},"environment":{}}',
            '{"resolved_support_cells":[],"resolved_support_cells":[]}',
            '{"source":{"commit":"a","commit":"a"}}',
        ):
            with self.subTest(text=text), tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / "input.json"
                path.write_text(text)
                with self.assertRaisesRegex(packaged_smoke.EvidenceError, "duplicate JSON member"):
                    packaged_smoke.load_json(path, "fixture")

    def test_direct_script_import_and_required_environment_cli_paths(self) -> None:
        script = REPO_ROOT / "verification/packaged_smoke.py"
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "support_environments.py").write_text("raise AssertionError('wrong validator')\n")
            environment = {**os.environ, "PYTHONPATH": str(root), "PYTHONDONTWRITEBYTECODE": "1"}
            result = subprocess.run([sys.executable, str(script), "--help"], cwd=root, env=environment, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertNotIn("dry-run", result.stdout)
            for command in ("complete-cell", "complete-server-cell"):
                for missing in ("--initial-environment", "--resume-environment", "--release-manifest"):
                    arguments = [sys.executable, str(script), command, "--cell", "SUP-IOS-MIN-001", "--killed-pid", "101"]
                    for flag in ("--repo-root", "--output", "--initial", "--resume", "--remote", "--server-verification",
                                 "--initial-environment", "--resume-environment", "--release-manifest"):
                        if flag != missing:
                            arguments.extend([flag, str(root)])
                    result = subprocess.run(arguments, cwd=root, env=environment, capture_output=True, text=True)
                    self.assertEqual(result.returncode, 2)
                    self.assertIn(missing, result.stderr)

    def test_cell_schema_declares_closed_profiles_and_canonical_formats(self) -> None:
        schema = packaged_smoke.load_json(REPO_ROOT / "verification/packaged-smoke-cell.schema.json", "cell schema")
        ids = set(packaged_smoke.required_cells(REPO_ROOT))
        self.assertFalse(schema["additionalProperties"])
        self.assertEqual(set(schema["properties"]["cell_id"]["enum"]), ids)
        self.assertIn("environment", schema["allOf"][0]["then"]["required"])
        self.assertNotIn("environment", schema["allOf"][0]["else"]["required"])
        profiles = {branch["properties"]["cell_id"]["const"]: branch["properties"]["environment"]["$ref"].split("/")[-1]
                    for branch in schema["oneOf"]}
        self.assertEqual(set(profiles), ids)
        self.assertEqual(len(schema["oneOf"]), 9)
        for cell_id, definition in profiles.items():
            profile = schema["$defs"][definition]
            measured = measured_environment(cell_id)
            self.assertEqual(profile["type"], "object")
            self.assertFalse(profile["additionalProperties"])
            self.assertEqual(set(profile["required"]), set(measured))
            self.assertEqual(set(profile["properties"]), set(measured))
            for field, value in measured.items():
                constraint = profile["properties"][field]
                if "$ref" in constraint:
                    constraint = schema["$defs"][constraint["$ref"].split("/")[-1]]
                with self.subTest(cell=cell_id, field=field):
                    if "const" in constraint:
                        self.assertEqual(value, constraint["const"])
                    else:
                        self.assertEqual(constraint["type"], "string")
                        self.assertIsNotNone(re.search(constraint["pattern"], value))
                        for bad in ("", "current", value + "\n", value + ";extra"):
                            self.assertIsNone(re.search(constraint["pattern"], bad))
        for definition, bad in (
            ("positiveInteger", "06"), ("positiveInteger", "0"), ("appleVersion", "027.0"),
            ("appleVersion", "27.0.0.0"), ("emulatorVersion", "37.2.12.0"),
            ("reactNativeVersion", "0.84.0"), ("systemImage", "system-images;android-37.1;google_apis;x86_64"),
            ("systemImage", "system-images;android-37.0;google_apis;arm64-v8a"),
        ):
            with self.subTest(definition=definition, bad=bad):
                self.assertIsNone(re.search(schema["$defs"][definition]["pattern"], bad))

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

    def environment_inputs(self, directory: Path, cell_id: str) -> tuple[Path, Path, Path]:
        initial = directory / "initial-environment.json"
        resume = directory / "resume-environment.json"
        manifest = directory / "release-manifest.json"
        packaged_smoke.write_json(initial, {"id": cell_id, "environment": measured_environment(cell_id)})
        packaged_smoke.write_json(resume, {"id": cell_id, "environment": measured_environment(cell_id)})
        packaged_smoke.write_json(manifest, {
            "source": {"commit": packaged_smoke.source_commit(REPO_ROOT)},
            "resolved_support_cells": selected_environments(),
        })
        return initial, resume, manifest

    def completion_arguments(self, directory: Path, cell_id: str) -> tuple:
        directory.mkdir(parents=True, exist_ok=True)
        initial, resume = directory / "initial.json", directory / "resume.json"
        remote, server = directory / "remote.json", directory / "server.json"
        artifact = directory / "artifact"
        artifact.write_bytes(b"fixture packaged bytes")
        if cell_id == "SUP-PG-LINUX-X64-001":
            packaged_smoke.write_json(initial, {
                "schema_version": 1, "phase": "initial", "status": "passed", "adapter_pid": 101, "push_digest": "a" * 64,
            })
            packaged_smoke.write_json(resume, {
                "schema_version": 1, "phase": "resume", "status": "passed", "adapter_pid": 202,
                "push_digest": "a" * 64, "replay_equal": True, "observed_customer_name": REMOTE_NAME,
            })
            resumed_write = packaged_smoke.SERVER_OFFLINE_WRITE
        else:
            packaged_smoke.write_json(initial, {
                "schema_version": 1, "phase": "initial", "status": "passed", "pid": 101,
                "pending_change_count": 2, "observed": INITIAL_OBSERVED,
            })
            packaged_smoke.write_json(resume, {
                "schema_version": 1, "phase": "resume", "status": "passed", "pid": 202,
                "pending_change_count": 0, "observed": RESUME_OBSERVED,
            })
            resumed_write = packaged_smoke.CLIENT_RESUMED_WRITE
        packaged_smoke.write_json(remote, {"schema_version": 1, "remote_value": REMOTE_NAME})
        packaged_smoke.write_json(server, packaged_smoke.server_verification(REMOTE_NAME, resumed_write))
        return (
            REPO_ROOT, cell_id, directory / "cell.json", initial, resume, 101,
            [artifact], packaged_smoke.hash_files([artifact]), remote, server,
            *self.environment_inputs(directory, cell_id),
        )

    def complete_fixture(self, arguments: tuple) -> None:
        complete = packaged_smoke.complete_server_cell if arguments[1] == "SUP-PG-LINUX-X64-001" else packaged_smoke.complete_cell
        complete(*arguments)

    def completed_summary(self, directory: Path) -> tuple[Path, Path]:
        cells_dir = directory / "cells"
        for cell_id in packaged_smoke.required_cells(REPO_ROOT):
            arguments = self.completion_arguments(directory / cell_id, cell_id)
            self.complete_fixture(arguments)
            packaged_smoke.write_json(cells_dir / f"{cell_id}.json", packaged_smoke.load_json(arguments[2], "cell"))
        output = directory / "summary.json"
        packaged_smoke.collect_summary(REPO_ROOT, cells_dir, output)
        return cells_dir, output

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
            cells_dir, _ = self.completed_summary(root)
            manifest_path = root / "release-manifest.json"
            roles = ["pg-extension", "adapter", "seed-tool", "kotlin-maven", "react-native-npm"]
            hashes = {role: str(index + 1) * 64 for index, role in enumerate(roles)}
            packaged_smoke.write_json(manifest_path, {
                "source": {"commit": packaged_smoke.source_commit(REPO_ROOT)},
                "resolved_support_cells": selected_environments(),
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
                "SUP-RN-IOS-MIN-001": [manifest_hash, hashes["react-native-npm"]],
                "SUP-RN-ANDROID-MIN-001": [hashes["kotlin-maven"], hashes["react-native-npm"]],
            }
            for cell_id, expected in cells.items():
                path = cells_dir / f"{cell_id}.json"
                receipt = packaged_smoke.load_json(path, f"cell {cell_id}")
                receipt["artifact_hashes"] = expected
                packaged_smoke.write_json(path, receipt)
            server_cell = "SUP-PG-LINUX-X64-001"
            server_receipt = packaged_smoke.load_json(cells_dir / f"{server_cell}.json", "server cell")
            validated = packaged_smoke.validate_manifest_cell(REPO_ROOT, server_cell, server_receipt, manifest_path)
            self.assertEqual(set(validated["artifact_hashes"]), set(cells[server_cell]))
            legacy_receipt = copy.deepcopy(server_receipt)
            legacy_receipt["schema_version"] = 1
            del legacy_receipt["lifecycle"]
            legacy_receipt["operations"] = [
                {"name": name, "status": "passed", "terminal": True, "test_count": 1}
                for name in packaged_smoke.LEGACY_SMOKE_OPERATIONS
            ]
            self.assertEqual(packaged_smoke.validate_manifest_cell(
                REPO_ROOT, server_cell, legacy_receipt, manifest_path,
            )["lifecycle"], {"status": "passed", "terminal": True, "test_count": 1})
            legacy_receipt["operations"][0]["test_count"] = True
            with self.assertRaises(packaged_smoke.EvidenceError):
                packaged_smoke.validate_manifest_cell(REPO_ROOT, server_cell, legacy_receipt, manifest_path)
            summary = {
                "schema_version": 2,
                "source_commit": packaged_smoke.source_commit(REPO_ROOT),
                "artifact_hashes": sorted({h for hs in cells.values() for h in hs}),
                "status": "passed",
                "resolved_support_cells": [
                    {"id": cell_id, "environment": measured_environment(cell_id)} for cell_id in sorted(cells)
                ],
                "cells": [
                    {"id": cell, "status": "passed", "terminal": True, "test_count": 1, "artifact_hashes": hs}
                    for cell, hs in cells.items()
                ],
            }
            summary_path = root / "summary.json"
            packaged_smoke.write_json(summary_path, summary)
            packaged_smoke.verify_summary(REPO_ROOT, summary_path, manifest_path)
            missing_receipt = cells_dir / "SUP-IOS-MIN-001.json"
            retained_bytes = missing_receipt.read_bytes()
            missing_receipt.unlink()
            with self.assertRaisesRegex(packaged_smoke.EvidenceError, "is missing or malformed"):
                packaged_smoke.verify_summary(REPO_ROOT, summary_path, manifest_path)
            missing_receipt.write_bytes(retained_bytes)
            original_summary = copy.deepcopy(summary)
            summary["cells"][0]["artifact_hashes"].append("f" * 64)
            summary["artifact_hashes"].append("f" * 64)
            changed_cell = cells_dir / f"{summary['cells'][0]['id']}.json"
            receipt = packaged_smoke.load_json(changed_cell, "changed cell")
            receipt["artifact_hashes"].append("f" * 64)
            packaged_smoke.write_json(changed_cell, receipt)
            packaged_smoke.write_json(summary_path, summary)
            with self.assertRaisesRegex(packaged_smoke.EvidenceError, "does not match sealed"):
                packaged_smoke.verify_summary(REPO_ROOT, summary_path, manifest_path)
            receipt["artifact_hashes"].pop()
            packaged_smoke.write_json(changed_cell, receipt)
            for cell_id, field, actual in (
                ("SUP-PG-LINUX-X64-001", "postgresql", "18.4"),
                ("SUP-IOS-CURRENT-001", "xcode", "27.0.1"),
                ("SUP-ANDROID-CURRENT-001", "system_image_revision", "7"),
                ("SUP-ANDROID-CURRENT-001", "emulator_version", "37.2.13"),
                ("SUP-ANDROID-CURRENT-001", "emulator_build", "16428234"),
                ("SUP-RN-IOS-CURRENT-001", "react_native", "0.88.1"),
            ):
                different = copy.deepcopy(original_summary)
                next(record for record in different["resolved_support_cells"] if record["id"] == cell_id)["environment"][field] = actual
                packaged_smoke.write_json(summary_path, different)
                with self.subTest(cell=cell_id, field=field), self.assertRaisesRegex(packaged_smoke.EvidenceError, "environments do not match sealed"):
                    packaged_smoke.verify_summary(REPO_ROOT, summary_path, manifest_path)

    def test_synthetic_summary_fixture_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory(prefix="packaged-smoke-structure.") as raw_directory:
            directory = Path(raw_directory)
            dry_path = directory / "dry.json"
            packaged_smoke.write_json(dry_path, {
                "schema_version": 2,
                "source_commit": packaged_smoke.source_commit(REPO_ROOT),
                "artifact_hashes": [hashlib.sha256(b"schema").hexdigest()],
                "cells": [{"id": cell_id, "status": "passed", "terminal": True, "test_count": 1,
                           "artifact_hashes": [hashlib.sha256(b"schema").hexdigest()]}
                          for cell_id in packaged_smoke.required_cells(REPO_ROOT)],
                "status": "passed",
                "dry_run": True,
            })
            with self.assertRaisesRegex(packaged_smoke.EvidenceError, "invalid members"):
                packaged_smoke.verify_summary(REPO_ROOT, dry_path)

    def test_missing_cells_become_terminal_failures(self) -> None:
        with tempfile.TemporaryDirectory(prefix="packaged-smoke-collect.") as raw_directory:
            directory = Path(raw_directory)
            output = directory / "summary.json"
            packaged_smoke.collect_summary(REPO_ROOT, directory / "cells", output)
            summary = packaged_smoke.load_json(output, "collected summary")
            self.assertEqual(summary["status"], "failed")
            self.assertEqual(len(summary["cells"]), len(packaged_smoke.required_cells(REPO_ROOT)))
            self.assertTrue(all(item["terminal"] is True for item in summary["cells"]))
            self.assertTrue(all(item["status"] == "failed" for item in summary["cells"]))
            self.assertEqual(summary["resolved_support_cells"], [])
            self.assertEqual(summary["missing_environment_cells"], sorted(packaged_smoke.required_cells(REPO_ROOT)))

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
                *self.environment_inputs(directory, cell_id),
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
                    *self.environment_inputs(directory, cell_id),
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
        self.assertEqual(len(cells), 9)

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
                    *self.environment_inputs(directory, packaged_smoke.required_cells(REPO_ROOT)[0]),
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
                            *self.environment_inputs(directory, cell_id),
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
                *self.environment_inputs(directory, packaged_smoke.required_cells(REPO_ROOT)[0]),
            )
            cell = packaged_smoke.load_json(cell_path, "server cell")
            self.assertEqual(cell["process_lifecycle"]["kind"], "server")
            packaged_smoke.validate_cell(cell, packaged_smoke.required_cells(REPO_ROOT)[0], packaged_smoke.source_commit(REPO_ROOT))

            resume["push_digest"] = "b" * 64
            packaged_smoke.write_json(resume_path, resume)
            with self.assertRaisesRegex(packaged_smoke.EvidenceError, "replay digest"):
                packaged_smoke.complete_server_cell(REPO_ROOT, packaged_smoke.required_cells(REPO_ROOT)[0], directory / "cell.json", initial_path, resume_path, 101, [artifact], packaged_smoke.hash_files([artifact]), remote, server, *self.environment_inputs(directory, packaged_smoke.required_cells(REPO_ROOT)[0]))
            resume["push_digest"] = digest
            resume["adapter_pid"] = 101
            packaged_smoke.write_json(resume_path, resume)
            with self.assertRaisesRegex(packaged_smoke.EvidenceError, "process replacement"):
                packaged_smoke.complete_server_cell(REPO_ROOT, packaged_smoke.required_cells(REPO_ROOT)[0], directory / "cell.json", initial_path, resume_path, 101, [artifact], packaged_smoke.hash_files([artifact]), remote, server, *self.environment_inputs(directory, packaged_smoke.required_cells(REPO_ROOT)[0]))
            resume["adapter_pid"] = 202
            resume["observed_customer_name"] = "Packaged server consumer"
            packaged_smoke.write_json(resume_path, resume)
            with self.assertRaisesRegex(packaged_smoke.EvidenceError, "did not pull the remote value"):
                packaged_smoke.complete_server_cell(REPO_ROOT, packaged_smoke.required_cells(REPO_ROOT)[0], directory / "cell.json", initial_path, resume_path, 101, [artifact], packaged_smoke.hash_files([artifact]), remote, server, *self.environment_inputs(directory, packaged_smoke.required_cells(REPO_ROOT)[0]))
            resume["observed_customer_name"] = REMOTE_NAME
            packaged_smoke.write_json(resume_path, resume)
            packaged_smoke.write_json(server, packaged_smoke.server_verification(REMOTE_NAME, {"customer_id": "other", "customer_name": "Packaged server offline"}))
            with self.assertRaisesRegex(packaged_smoke.EvidenceError, "server verification does not confirm"):
                packaged_smoke.complete_server_cell(REPO_ROOT, packaged_smoke.required_cells(REPO_ROOT)[0], directory / "cell.json", initial_path, resume_path, 101, [artifact], packaged_smoke.hash_files([artifact]), remote, server, *self.environment_inputs(directory, packaged_smoke.required_cells(REPO_ROOT)[0]))



if __name__ == "__main__":
    unittest.main()
