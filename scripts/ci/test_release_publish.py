#!/usr/bin/env python3
"""Test immutable publication state classification."""

from __future__ import annotations

import importlib.util
import hashlib
import json
import os
import subprocess
import sys
import tempfile
import textwrap
import threading
import unittest
import urllib.parse
import zipfile
from datetime import datetime, timezone
from email import policy
from email.parser import BytesParser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location("release_publish", ROOT / "scripts/release-publish.py")
assert SPEC is not None and SPEC.loader is not None
release_publish = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(release_publish)


def release_step_command(name: str) -> str:
    lines = (ROOT / ".github/workflows/release.yml").read_text(encoding="utf-8").splitlines()
    start = lines.index(f"      - name: {name}")
    run = next(index for index in range(start, len(lines)) if lines[index] == "        run: |")
    end = next(index for index in range(run + 1, len(lines)) if lines[index] and not lines[index].startswith("          "))
    return textwrap.dedent("\n".join(lines[run + 1:end]))


class PublicationStateTests(unittest.TestCase):
    def setUp(self) -> None:
        self.commit = "a" * 40
        self.version = "1.2.3"
        self.identity = {
            "version": self.version,
            "source_commit": self.commit,
            "root_tag": "v1.2.3",
            "go_tag": "api/go/v1.2.3",
            "github_assets": {"server": "1" * 64, "manifest": "2" * 64},
            "maven_bundle": {"sha256": "3" * 64, "deployment_name": f"synchro-1.2.3-{self.commit}"},
            "maven_entries": {"fit/trainstar/synchro/1.2.3/synchro-1.2.3.aar": "4" * 64},
            "npm": {"sha256": "5" * 64},
        }

    def state(self) -> dict[str, object]:
        return {
            "tags": {"v1.2.3": None, "api/go/v1.2.3": None},
            "github": None,
            "maven": {"public_files": {}},
            "npm": {"sha256": None, "dist_tags": {}, "provenance": False},
        }

    def test_candidate_identity_accepts_exact_commit_and_version(self) -> None:
        release_publish.validate_candidate_identity(self.commit, self.version)

    def test_release_dispatch_requires_master(self) -> None:
        command = release_step_command("Require dispatch from master")
        for ref in (
            "refs/heads/master",
            "refs/heads/dev",
            "refs/heads/main",
            "refs/heads/master-copy",
            "refs/heads/release/1.2.3",
            "refs/tags/v1.2.3",
            "refs/pull/1/merge",
            "",
        ):
            with self.subTest(ref=ref):
                result = subprocess.run(
                    ["bash", "-eu", "-c", command],
                    env={**os.environ, "GITHUB_REF": ref},
                    capture_output=True, text=True, timeout=5, check=False,
                )
                self.assertEqual(result.returncode == 0, ref == "refs/heads/master")

    def test_candidate_identity_rejects_invalid_commit_lengths(self) -> None:
        for commit in ("a" * 39, "a" * 41):
            with self.subTest(length=len(commit)):
                with self.assertRaisesRegex(release_publish.PublicationError, "source commit"):
                    release_publish.validate_candidate_identity(commit, self.version)

    def test_candidate_identity_rejects_noncanonical_values(self) -> None:
        invalid = (
            ("A" * 40, self.version),
            (self.commit, "v1.2.3"),
            (self.commit, "1.2"),
        )
        for commit, version in invalid:
            with self.subTest(commit=commit, version=version):
                with self.assertRaises(release_publish.PublicationError):
                    release_publish.validate_candidate_identity(commit, version)

    def test_new_candidate_starts_with_tags(self) -> None:
        result = release_publish.classify_publication(self.identity, self.state())
        self.assertEqual(result["next_operation"], "create-tags")
        self.assertEqual(result["source_tags"], "absent")

    def test_partial_tag_resumes_missing_tag(self) -> None:
        state = self.state()
        state["tags"]["v1.2.3"] = self.commit
        result = release_publish.classify_publication(self.identity, state)
        self.assertEqual(result["source_tags"], "partial")
        self.assertEqual(result["next_operation"], "create-tags")

    def test_partial_draft_resumes_assets(self) -> None:
        state = self.state()
        state["tags"] = {"v1.2.3": self.commit, "api/go/v1.2.3": self.commit}
        state["github"] = {"draft": True, "latest": False, "assets": {"server": "1" * 64}}
        result = release_publish.classify_publication(self.identity, state)
        self.assertEqual(result["github"], "draft-partial")
        self.assertEqual(result["next_operation"], "publish-github")

    def test_absent_public_maven_requires_publication(self) -> None:
        state = self.state()
        state["tags"] = {"v1.2.3": self.commit, "api/go/v1.2.3": self.commit}
        state["github"] = {"draft": False, "latest": False, "assets": self.identity["github_assets"]}
        result = release_publish.classify_publication(self.identity, state)
        self.assertEqual(result["maven"], "absent")
        self.assertEqual(result["next_operation"], "publish-maven")

    def test_public_maven_state_rejects_private_deployment_claims(self) -> None:
        state = self.state()
        state["maven"]["deployment_state"] = "PUBLISHED"
        with self.assertRaises(release_publish.PublicationError):
            release_publish.classify_publication(self.identity, state)

    def test_public_maven_identity_requires_complete_matching_bytes(self) -> None:
        state = self.state()
        expected = {
            "fit/trainstar/synchro/1.2.3/synchro-1.2.3.aar": "4" * 64,
            "fit/trainstar/synchro/1.2.3/synchro-1.2.3.pom": "6" * 64,
        }
        self.identity["maven_entries"] = expected
        invalid = (
            {next(iter(expected)): "4" * 64},
            {**expected, next(iter(expected)): "0" * 64},
            {**expected, "unexpected.jar": "7" * 64},
        )
        for files in invalid:
            with self.subTest(files=files):
                state["maven"]["public_files"] = files
                with self.assertRaises(release_publish.PublicationError):
                    release_publish.classify_publication(self.identity, state)
        state["maven"]["public_files"] = expected
        self.assertEqual(release_publish.classify_publication(self.identity, state)["maven"], "published")

    @mock.patch.object(release_publish, "central_json", side_effect=AssertionError("public observation used private state"))
    @mock.patch.object(release_publish, "request_json", return_value=None)
    @mock.patch.object(release_publish, "request_bytes")
    def test_public_observer_returns_only_verified_public_maven_files(
        self, request_bytes: mock.Mock, request_json: mock.Mock, central_json: mock.Mock
    ) -> None:
        payload = b"authored public Maven bytes"
        relative = next(iter(self.identity["maven_entries"]))
        self.identity["maven_entries"] = {relative: hashlib.sha256(payload).hexdigest()}
        request_bytes.return_value = payload
        state = release_publish.observe_public(self.identity, "trainstar/synchro", None)
        self.assertEqual(state["maven"], {"public_files": self.identity["maven_entries"]})
        self.assertEqual(release_publish.classify_publication(self.identity, state)["maven"], "published")
        request_bytes.assert_called_once_with(f"{release_publish.MAVEN_BASE}/{relative}")
        central_json.assert_not_called()

    def test_matching_publication_promotes_latest(self) -> None:
        state = self.state()
        state["tags"] = {"v1.2.3": self.commit, "api/go/v1.2.3": self.commit}
        state["github"] = {"draft": False, "latest": False, "assets": self.identity["github_assets"]}
        state["maven"]["public_files"] = self.identity["maven_entries"]
        state["npm"] = {"sha256": self.identity["npm"]["sha256"], "dist_tags": {"latest": "1.2.3"}, "provenance": True}
        result = release_publish.classify_publication(self.identity, state)
        self.assertEqual(result["next_operation"], "promote-github")

    def test_public_maven_publishes_npm_last(self) -> None:
        state = self.state()
        state["tags"] = {"v1.2.3": self.commit, "api/go/v1.2.3": self.commit}
        state["github"] = {"draft": False, "latest": False, "assets": self.identity["github_assets"]}
        state["maven"]["public_files"] = self.identity["maven_entries"]
        result = release_publish.classify_publication(self.identity, state)
        self.assertEqual(result["next_operation"], "publish-npm")

    def test_complete_state_is_terminal(self) -> None:
        state = self.state()
        state["tags"] = {"v1.2.3": self.commit, "api/go/v1.2.3": self.commit}
        state["github"] = {"draft": False, "latest": True, "assets": self.identity["github_assets"]}
        state["maven"]["public_files"] = self.identity["maven_entries"]
        state["npm"] = {"sha256": self.identity["npm"]["sha256"], "dist_tags": {"latest": "1.2.3"}, "provenance": True}
        result = release_publish.classify_publication(self.identity, state)
        self.assertTrue(result["complete"])
        self.assertEqual(result["next_operation"], "complete")

    def test_missing_source_tag_is_not_terminal(self) -> None:
        state = self.state()
        state["tags"] = {"v1.2.3": self.commit, "api/go/v1.2.3": None}
        state["github"] = {"draft": False, "latest": True, "assets": self.identity["github_assets"]}
        state["maven"]["public_files"] = self.identity["maven_entries"]
        state["npm"] = {
            "sha256": self.identity["npm"]["sha256"],
            "dist_tags": {"latest": "1.2.3"},
            "provenance": True,
        }
        result = release_publish.classify_publication(self.identity, state)
        self.assertFalse(result["complete"])
        self.assertEqual(result["next_operation"], "create-tags")

    def test_wrong_tag_commit_fails(self) -> None:
        state = self.state()
        state["tags"]["v1.2.3"] = "b" * 40
        with self.assertRaisesRegex(release_publish.PublicationError, "different commit"):
            release_publish.classify_publication(self.identity, state)

    def test_wrong_public_bytes_fail(self) -> None:
        state = self.state()
        state["npm"] = {"sha256": "f" * 64, "dist_tags": {"latest": "1.2.3"}, "provenance": True}
        with self.assertRaisesRegex(release_publish.PublicationError, "npm package bytes differ"):
            release_publish.classify_publication(self.identity, state)

    def test_candidate_npm_state_is_not_recoverable(self) -> None:
        state = self.state()
        state["npm"] = {"sha256": self.identity["npm"]["sha256"], "dist_tags": {"candidate": "1.2.3"}, "provenance": True}
        with self.assertRaisesRegex(release_publish.PublicationError, "candidate dist-tag is obsolete"):
            release_publish.classify_publication(self.identity, state)

    def test_existing_npm_version_without_latest_fails(self) -> None:
        state = self.state()
        state["npm"] = {"sha256": self.identity["npm"]["sha256"], "dist_tags": {"latest": "1.2.2"}, "provenance": True}
        with self.assertRaisesRegex(release_publish.PublicationError, "not published under latest"):
            release_publish.classify_publication(self.identity, state)

    def test_existing_npm_version_without_provenance_fails(self) -> None:
        state = self.state()
        state["npm"] = {"sha256": self.identity["npm"]["sha256"], "dist_tags": {"latest": "1.2.3"}, "provenance": False}
        with self.assertRaisesRegex(release_publish.PublicationError, "provenance is missing"):
            release_publish.classify_publication(self.identity, state)

    def test_npm_provenance_requires_slsa_registry_attestation(self) -> None:
        self.assertTrue(release_publish.has_npm_provenance({
            "attestations": {
                "url": "https://registry.npmjs.org/-/npm/v1/attestations/%40trainstar%2Fsynchro-react-native@1.2.3",
                "provenance": {"predicateType": "https://slsa.dev/provenance/v1"},
            },
        }))
        self.assertFalse(release_publish.has_npm_provenance({
            "attestations": {
                "url": "https://example.invalid/-/npm/v1/attestations/package@1.2.3",
                "provenance": {"predicateType": "https://slsa.dev/provenance/v1"},
            },
        }))
        self.assertFalse(release_publish.has_npm_provenance({
            "attestations": {
                "url": "https://registry.npmjs.org/-/npm/v1/attestations/package@1.2.3",
                "provenance": {"predicateType": "https://example.invalid/provenance/v1"},
            },
        }))

    def test_published_partial_github_release_fails(self) -> None:
        state = self.state()
        state["github"] = {"draft": False, "latest": False, "assets": {"server": "1" * 64}}
        with self.assertRaisesRegex(release_publish.PublicationError, "incomplete assets"):
            release_publish.classify_publication(self.identity, state)

    def test_recovered_public_github_release_does_not_publish_again(self) -> None:
        for latest, expected_status in ((False, "public"), (True, "public-latest")):
            with self.subTest(latest=latest):
                state = self.state()
                state["tags"] = {"v1.2.3": self.commit, "api/go/v1.2.3": self.commit}
                state["github"] = {
                    "draft": False,
                    "latest": latest,
                    "assets": self.identity["github_assets"],
                }
                result = release_publish.classify_publication(self.identity, state)
                self.assertEqual(result["github"], expected_status)
                self.assertEqual(result["next_operation"], "publish-maven")

    def test_selects_one_deterministic_central_deployment(self) -> None:
        value = {"deployments": [{"deploymentId": "one", "deploymentName": "wanted", "deploymentState": "VALIDATED"}]}
        self.assertEqual(
            release_publish.select_central(value, "wanted"),
            {"deployment_id": "one", "deployment_name": "wanted", "deployment_state": "VALIDATED"},
        )

    def test_duplicate_central_deployments_fail(self) -> None:
        value = {"deployments": [
            {"deploymentId": "one", "deploymentName": "wanted", "deploymentState": "VALIDATED"},
            {"deploymentId": "two", "deploymentName": "wanted", "deploymentState": "PENDING"},
        ]}
        with self.assertRaisesRegex(release_publish.PublicationError, "duplicate"):
            release_publish.select_central(value, "wanted")

    @mock.patch.object(release_publish, "central_json")
    def test_lists_every_central_page_with_documented_size_parameter(self, central_json: mock.Mock) -> None:
        central_json.side_effect = [
            {
                "deployments": [{"deploymentId": "one", "deploymentName": "wanted-a", "deploymentState": "VALIDATED"}],
                "page": 0,
                "pageSize": 1,
                "pageCount": 2,
                "totalResultCount": 2,
            },
            {
                "deployments": [{"deploymentId": "two", "deploymentName": "wanted", "deploymentState": "VALIDATED"}],
                "page": 1,
                "pageSize": 1,
                "pageCount": 2,
                "totalResultCount": 2,
            },
        ]
        value = release_publish.list_central_deployments("wanted")
        self.assertEqual(release_publish.select_central(value, "wanted")["deployment_id"], "two")
        self.assertEqual(central_json.call_count, 2)
        for page, call in enumerate(central_json.call_args_list):
            path = call.args[0]
            query = urllib.parse.parse_qs(urllib.parse.urlsplit(path).query)
            self.assertEqual(query["deploymentName"], ["wanted"])
            self.assertEqual(query["page"], [str(page)])
            self.assertEqual(query["size"], ["100"])
            self.assertNotIn("pageSize", query)
            self.assertEqual(call.kwargs, {"method": "GET"})

    @mock.patch.object(release_publish, "central_json")
    def test_rejects_partial_central_pagination(self, central_json: mock.Mock) -> None:
        central_json.return_value = {
            "deployments": [],
            "page": 0,
            "pageSize": 100,
            "pageCount": 1,
            "totalResultCount": 1,
        }
        with self.assertRaisesRegex(release_publish.PublicationError, "incomplete"):
            release_publish.list_central_deployments("wanted")

    def test_release_workflow_executes_private_recovery_effects(self) -> None:
        command = release_step_command("Resolve or upload and validate Maven deployment")
        command = command.replace("${{ needs.candidate.outputs.release_dir_name }}", "fixture")
        self.assertNotIn("${{", command)

        with tempfile.TemporaryDirectory(prefix="synchro-publication-effects-") as directory:
            root = Path(directory)
            release_dir = root / "dist/releases/fixture"
            release_dir.mkdir(parents=True)
            distributions = []
            for role, name in (
                ("pg-extension", "postgres.tar.gz"),
                ("adapter", "adapter.tar.gz"),
                ("seed-tool", "seed.tar.gz"),
                ("kotlin-maven", "maven.zip"),
                ("react-native-npm", "react-native.tgz"),
            ):
                path = release_dir / name
                if role == "kotlin-maven":
                    with zipfile.ZipFile(path, "w") as archive:
                        archive.writestr("fit/trainstar/synchro/1.2.3/synchro-1.2.3.aar", b"sealed Maven payload")
                else:
                    path.write_bytes(f"sealed {role} bytes".encode())
                distributions.append({
                    "role": role, "kind": "file", "path": name,
                    "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                })
            (release_dir / "release-manifest.json").write_text(json.dumps({
                "schema_version": 1,
                "release_version": self.version,
                "source": {"commit": self.commit, "source_tags": [f"api/go/v{self.version}", f"v{self.version}"]},
                "distributions": distributions,
            }), encoding="utf-8")
            (release_dir / "SHA256SUMS").write_text(
                "".join(f"{item['sha256']}  {item['path']}\n" for item in distributions), encoding="utf-8"
            )
            (release_dir / "sbom.spdx.json").write_text('{"spdxVersion":"SPDX-2.3"}', encoding="utf-8")
            identity = release_publish.identity_for_directory(release_dir)
            bundle = (release_dir / "maven.zip").read_bytes()
            name = identity["maven_bundle"]["deployment_name"]
            tools = root / "tools"
            tools.mkdir()
            python_proxy = tools / "python3"
            python_proxy.write_text(f"#!{sys.executable}\n" + textwrap.dedent("""
                import importlib.util
                import os
                import sys
                assert sys.argv[1] == "scripts/release-publish.py"
                spec = importlib.util.spec_from_file_location("publisher", os.environ["TEST_PUBLISHER"])
                publisher = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(publisher)
                publisher.CENTRAL_API = os.environ["TEST_CENTRAL_URL"]
                sys.argv = sys.argv[1:]
                raise SystemExit(publisher.main())
                """), encoding="utf-8")
            python_proxy.chmod(0o755)
            sleeper = tools / "sleep"
            sleeper.write_text('#!/bin/sh\nprintf "%s\\n" "$1" >> "$TEST_SLEEP_LOG"\n', encoding="utf-8")
            sleeper.chmod(0o755)

            for initial in (None, "PENDING", "VALIDATING", "VALIDATED", "FAILED", "PUBLISHING", "PUBLISHED", "PUBLIC"):
                with self.subTest(initial=initial):
                    requests = []
                    uploads = []
                    errors = []

                    class Handler(BaseHTTPRequestHandler):
                        def log_message(self, *_args: object) -> None:
                            return

                        def respond(self, payload: bytes, status: int = 200) -> None:
                            self.send_response(status)
                            self.send_header("Content-Length", str(len(payload)))
                            self.end_headers()
                            self.wfile.write(payload)

                        def do_GET(self) -> None:
                            parsed = urllib.parse.urlsplit(self.path)
                            requests.append(("GET", parsed.path))
                            query = urllib.parse.parse_qs(parsed.query)
                            if parsed.path != "/deployments" or query.get("deploymentName") != [name]:
                                errors.append(("GET", self.path))
                                self.respond(b"unexpected request", 400)
                                return
                            deployments = [] if initial is None else [{
                                "deploymentId": "old", "deploymentName": name, "deploymentState": initial,
                            }]
                            self.respond(json.dumps({
                                "deployments": deployments, "page": 0, "pageSize": 100,
                                "pageCount": 1, "totalResultCount": len(deployments),
                            }).encode())

                        def do_POST(self) -> None:
                            parsed = urllib.parse.urlsplit(self.path)
                            query = urllib.parse.parse_qs(parsed.query)
                            if parsed.path == "/upload":
                                requests.append(("POST", "/upload"))
                                uploads.append((
                                    query,
                                    self.rfile.read(int(self.headers["Content-Length"])),
                                    self.headers["Content-Type"],
                                ))
                                self.respond(b"new")
                            elif parsed.path == "/status" and query.get("id") in (["old"], ["new"]):
                                requests.append(("POST", "/status/" + query["id"][0]))
                                self.respond(b'{"deploymentState":"VALIDATED"}')
                            else:
                                errors.append(("POST", self.path))
                                self.respond(b"unexpected request", 400)

                        def do_DELETE(self) -> None:
                            requests.append(("DELETE", self.path))
                            if self.path != "/deployment/old":
                                errors.append(("DELETE", self.path))
                                self.respond(b"unexpected request", 400)
                                return
                            self.respond(b"")

                    runner = root / str(initial)
                    runner.mkdir()
                    state = self.state()
                    state["maven"]["public_files"] = identity["maven_entries"] if initial == "PUBLIC" else {}
                    (runner / "public-github-classification.json").write_text(
                        json.dumps(release_publish.classify_publication(identity, state)), encoding="utf-8"
                    )
                    sleep_log = runner / "sleep.log"
                    sleep_log.touch()
                    with ThreadingHTTPServer(("127.0.0.1", 0), Handler) as server:
                        thread = threading.Thread(target=server.serve_forever)
                        thread.start()
                        try:
                            result = subprocess.run(
                                ["bash", "-c", command], cwd=root,
                                env={
                                    **os.environ,
                                    "PATH": str(tools) + os.pathsep + os.environ["PATH"],
                                    "GITHUB_WORKSPACE": str(root),
                                    "RUNNER_TEMP": str(runner),
                                    "GITHUB_OUTPUT": str(runner / "outputs"),
                                    "MAVEN_CENTRAL_USERNAME": "fixture-user",
                                    "MAVEN_CENTRAL_PASSWORD": "fixture-password",
                                    "TEST_PUBLISHER": str(ROOT / "scripts/release-publish.py"),
                                    "TEST_CENTRAL_URL": f"http://127.0.0.1:{server.server_port}",
                                    "TEST_SLEEP_LOG": str(sleep_log),
                                },
                                text=True, capture_output=True, timeout=20, check=False,
                            )
                        finally:
                            server.shutdown()
                            thread.join()
                    self.assertEqual(result.returncode, 0, result.stderr)
                    self.assertEqual(errors, [])
                    expected = [] if initial == "PUBLIC" else [("GET", "/deployments")]
                    if initial in {"PENDING", "VALIDATING"}:
                        expected.append(("POST", "/status/old"))
                    replaced = initial in {"PENDING", "VALIDATING", "VALIDATED", "FAILED"}
                    if replaced:
                        expected.append(("DELETE", "/deployment/old"))
                    uploaded = initial is None or replaced
                    if uploaded:
                        expected.extend([("POST", "/upload"), ("POST", "/status/new")])
                    self.assertEqual(requests, expected)
                    self.assertEqual(len(uploads), int(uploaded))
                    if uploaded:
                        self.assertEqual(uploads[0][0], {"name": [name], "publishingType": ["USER_MANAGED"]})
                        message = BytesParser(policy=policy.default).parsebytes(
                            f"Content-Type: {uploads[0][2]}\r\n\r\n".encode() + uploads[0][1]
                        )
                        parts = list(message.iter_parts())
                        self.assertEqual(len(parts), 1)
                        self.assertEqual(parts[0].get_param("name", header="Content-Disposition"), "bundle")
                        self.assertEqual(parts[0].get_payload(decode=True), bundle)
                    operation = json.loads((runner / "maven-operation.json").read_text(encoding="utf-8"))
                    self.assertEqual(operation["bundle_sha256"], hashlib.sha256(bundle).hexdigest())
                    self.assertEqual(operation["deployment_name"], name)
                    self.assertEqual(operation["deployment_id"], "new" if uploaded else "" if initial == "PUBLIC" else "old")
                    self.assertEqual(operation["deployment_state"], "VALIDATED" if uploaded else initial)
                    self.assertEqual(
                        operation["verification"],
                        "sealed-upload" if uploaded else "public-repository" if initial == "PUBLIC" else "awaiting-public-byte-verification",
                    )
                    expected_waits = int(uploaded) + int(initial in {"PENDING", "VALIDATING"})
                    self.assertEqual(sleep_log.read_text(encoding="utf-8").splitlines(), ["15"] * expected_waits)

    def receipt(self, digest: str = "6" * 64, attempt: str = "2") -> dict[str, str]:
        return {
            "artifact_id": "345",
            "artifact_digest": digest,
            "run_id": "123",
            "run_attempt": attempt,
            "expires_at": "2026-12-01T00:00:00Z",
        }

    def artifact(self, digest: str = "sha256:" + "6" * 64) -> dict[str, object]:
        return {
            "id": 345,
            "name": "sealed-release",
            "expired": False,
            "digest": digest,
            "workflow_run": {"id": 123},
        }

    def manifest(self, attempt: int = 2) -> dict[str, object]:
        return {
            "build": {
                "run_id": "123",
                "run_attempt": attempt,
                "workflow_path": ".github/workflows/release.yml",
            },
        }

    def test_receipt_accepts_bare_receipt_and_prefixed_api_digest(self) -> None:
        result = release_publish.verify_sealed_receipt(
            self.receipt(),
            self.artifact(),
            self.manifest(),
            "123",
            now=datetime(2026, 9, 14, tzinfo=timezone.utc),
        )
        self.assertEqual(result["artifact_digest"], "6" * 64)
        self.assertEqual(result["run_attempt"], "2")

    def test_receipt_accepts_prefixed_receipt_and_bare_api_digest(self) -> None:
        result = release_publish.verify_sealed_receipt(
            self.receipt("sha256:" + "6" * 64),
            self.artifact("6" * 64),
            self.manifest(),
            "123",
            now=datetime(2026, 9, 14, tzinfo=timezone.utc),
        )
        self.assertEqual(result["artifact_digest"], "6" * 64)

    def test_receipt_rejects_malformed_digest(self) -> None:
        with self.assertRaisesRegex(release_publish.PublicationError, "receipt digest is invalid"):
            release_publish.verify_sealed_receipt(
                self.receipt("sha512:" + "6" * 64),
                self.artifact(),
                self.manifest(),
                "123",
                now=datetime(2026, 9, 14, tzinfo=timezone.utc),
            )

    def test_receipt_rejects_digest_mismatch(self) -> None:
        with self.assertRaisesRegex(release_publish.PublicationError, "digest differs"):
            release_publish.verify_sealed_receipt(
                self.receipt(),
                self.artifact("sha256:" + "7" * 64),
                self.manifest(),
                "123",
                now=datetime(2026, 9, 14, tzinfo=timezone.utc),
            )

    def test_receipt_uses_original_artifact_attempt_not_latest_rerun(self) -> None:
        result = release_publish.verify_sealed_receipt(
            self.receipt(attempt="2"),
            self.artifact(),
            self.manifest(attempt=2),
            "123",
            now=datetime(2026, 9, 14, tzinfo=timezone.utc),
        )
        self.assertEqual(result["run_attempt"], "2")

    def test_receipt_rejects_artifact_attempt_mismatch(self) -> None:
        with self.assertRaisesRegex(release_publish.PublicationError, "another workflow attempt"):
            release_publish.verify_sealed_receipt(
                self.receipt(attempt="2"),
                self.artifact(),
                self.manifest(attempt=3),
                "123",
                now=datetime(2026, 9, 14, tzinfo=timezone.utc),
            )

    def test_receipt_rejects_wrong_recovery_run(self) -> None:
        with self.assertRaisesRegex(release_publish.PublicationError, "another workflow run"):
            release_publish.verify_sealed_receipt(
                self.receipt(),
                self.artifact(),
                self.manifest(),
                "124",
                now=datetime(2026, 9, 14, tzinfo=timezone.utc),
            )

if __name__ == "__main__":
    unittest.main()
