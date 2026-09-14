#!/usr/bin/env python3
"""Test immutable publication state classification."""

from __future__ import annotations

import importlib.util
import os
import re
import socket
import subprocess
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location("release_publish", ROOT / "scripts/release-publish.py")
assert SPEC is not None and SPEC.loader is not None
release_publish = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(release_publish)


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
            "maven": {
                "deployment_id": None,
                "deployment_name": None,
                "deployment_state": None,
                "bundle_sha256": None,
                "public_files": {},
            },
            "npm": {"sha256": None, "dist_tags": {}, "provenance": False},
        }

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

    def test_validated_maven_resumes_publication(self) -> None:
        state = self.state()
        state["tags"] = {"v1.2.3": self.commit, "api/go/v1.2.3": self.commit}
        state["github"] = {"draft": False, "latest": False, "assets": self.identity["github_assets"]}
        state["maven"] = {
            "deployment_id": "deployment-1",
            "deployment_name": self.identity["maven_bundle"]["deployment_name"],
            "deployment_state": "VALIDATED",
            "bundle_sha256": self.identity["maven_bundle"]["sha256"],
            "public_files": {},
        }
        result = release_publish.classify_publication(self.identity, state)
        self.assertEqual(result["maven"], "validated")
        self.assertEqual(result["next_operation"], "publish-maven")

    def test_publishing_maven_resumes_public_verification(self) -> None:
        state = self.state()
        state["tags"] = {"v1.2.3": self.commit, "api/go/v1.2.3": self.commit}
        state["github"] = {"draft": False, "latest": False, "assets": self.identity["github_assets"]}
        state["maven"] = {
            "deployment_id": "deployment-1",
            "deployment_name": self.identity["maven_bundle"]["deployment_name"],
            "deployment_state": "PUBLISHING",
            "bundle_sha256": self.identity["maven_bundle"]["sha256"],
            "public_files": {},
        }
        result = release_publish.classify_publication(self.identity, state)
        self.assertEqual(result["maven"], "publishing")
        self.assertEqual(result["next_operation"], "publish-maven")

    def test_published_maven_waits_for_public_repository(self) -> None:
        state = self.state()
        state["tags"] = {"v1.2.3": self.commit, "api/go/v1.2.3": self.commit}
        state["github"] = {"draft": False, "latest": False, "assets": self.identity["github_assets"]}
        state["maven"] = {
            "deployment_id": "deployment-1",
            "deployment_name": self.identity["maven_bundle"]["deployment_name"],
            "deployment_state": "PUBLISHED",
            "bundle_sha256": self.identity["maven_bundle"]["sha256"],
            "public_files": {},
        }
        result = release_publish.classify_publication(self.identity, state)
        self.assertEqual(result["maven"], "published-pending-public")
        self.assertEqual(result["next_operation"], "publish-maven")

    def test_failed_maven_deployment_stops_recovery(self) -> None:
        state = self.state()
        state["maven"] = {
            "deployment_id": "deployment-1",
            "deployment_name": self.identity["maven_bundle"]["deployment_name"],
            "deployment_state": "FAILED",
            "bundle_sha256": self.identity["maven_bundle"]["sha256"],
            "public_files": {},
        }
        with self.assertRaisesRegex(release_publish.PublicationError, "deployment failed"):
            release_publish.classify_publication(self.identity, state)

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

    def test_fixture_cleanup_preserves_original_failure_and_remote_state(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            fake_bin = root / "bin"
            fake_bin.mkdir()
            ssh_count = root / "ssh-count"
            ssh_log = root / "ssh-log"
            fake_ssh = fake_bin / "ssh"
            fake_ssh.write_text(
                """#!/bin/sh
set -eu
count_file=${FAKE_SSH_COUNT:?}
log_file=${FAKE_SSH_LOG:?}
count=0
[ ! -f "$count_file" ] || count=$(cat "$count_file")
count=$((count + 1))
printf '%s\n' "$count" > "$count_file"
printf '%s\n' "$*" >> "$log_file"
cat >/dev/null
case "$count" in
  1) exit 0 ;;
  2) printf '%s\n' "simulated upload failure" >&2; exit 41 ;;
  3) printf '%s\n' "simulated cleanup ssh failure" >&2; exit 42 ;;
  *) exit 99 ;;
esac
""",
                encoding="utf-8",
            )
            fake_ssh.chmod(0o700)
            release = root / "release/artifacts"
            release.mkdir(parents=True)
            for name in (
                "synchro-pg-pg18-ubuntu24.04-linux-x64-1.2.3.tar.gz",
                "synchrod-pg-linux-x64-1.2.3",
                "synchro-seed-linux-x64-1.2.3",
            ):
                (release / name).write_bytes(name.encode("ascii"))
            provisioner = root / "synchro-local-postgres"
            provisioner.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
            ssh_directory = root / "ssh"
            environment = os.environ.copy()
            environment.update(
                {
                    "PATH": f"{fake_bin}:{environment['PATH']}",
                    "FAKE_SSH_COUNT": str(ssh_count),
                    "FAKE_SSH_LOG": str(ssh_log),
                    "GITHUB_RUN_ID": "123",
                    "GITHUB_RUN_ATTEMPT": "1",
                    "GITHUB_JOB": "candidate-swift",
                    "GITHUB_WORKSPACE": str(ROOT),
                    "RUNNER_TEMP": str(root),
                    "RELEASE_FIXTURE_SSH_PRIVATE_KEY": "fixture-private-key",
                    "RELEASE_FIXTURE_SSH_KNOWN_HOSTS": "fixture.example ssh-ed25519 AAAAfixture",
                }
            )
            setup = subprocess.run(
                [
                    "sh",
                    str(ROOT / "scripts/ci/release-linux-fixture.sh"),
                    "setup-ssh",
                    str(ssh_directory),
                ],
                check=False,
                capture_output=True,
                text=True,
                env=environment,
            )
            self.assertEqual(setup.returncode, 0, setup.stderr)
            self.assertEqual(ssh_directory.stat().st_mode & 0o777, 0o700)
            self.assertEqual((ssh_directory / "private-key").stat().st_mode & 0o777, 0o600)
            self.assertEqual((ssh_directory / "known-hosts").stat().st_mode & 0o777, 0o600)
            result = subprocess.run(
                [
                    "sh",
                    str(ROOT / "scripts/ci/release-linux-fixture.sh"),
                    "--known-hosts",
                    str(ssh_directory / "known-hosts"),
                    "--key",
                    str(ssh_directory / "private-key"),
                    "--user",
                    "fixture",
                    "--host",
                    "fixture.example",
                    "--remote-root",
                    "/srv/synchro",
                    "--pg18-bin-dir",
                    "/usr/lib/postgresql/18/bin",
                    "--release-dir",
                    str(release.parent),
                    "--version",
                    "1.2.3",
                    "--provisioner",
                    str(provisioner),
                    "--cell",
                    "CI-SWIFT",
                    "--",
                    "true",
                ],
                check=False,
                capture_output=True,
                text=True,
                env=environment,
            )
            self.assertEqual(result.returncode, 41)
            self.assertIn("simulated cleanup ssh failure", result.stderr)
            self.assertIn("retained /srv/synchro/run-123-1-candidate-swift-CI-SWIFT", result.stderr)
            self.assertIn("pre-attach", ssh_log.read_text(encoding="utf-8").splitlines()[2])
            self.assertTrue(os.access(provisioner, os.X_OK))

    def test_fixture_selects_and_exports_distinct_local_adapter_port(self) -> None:
        script = ROOT / "scripts/ci/release-linux-fixture.sh"
        with socket.socket() as postgres, socket.socket() as http:
            postgres.bind(("127.0.0.1", 0))
            http.bind(("127.0.0.1", 0))
            excluded = {postgres.getsockname()[1], http.getsockname()[1]}
            result = subprocess.run(
                ["sh", str(script), "select-free-port", *(str(port) for port in sorted(excluded))],
                check=False,
                capture_output=True,
                text=True,
            )
        self.assertEqual(result.returncode, 0, result.stderr)
        selected = int(result.stdout.strip())
        self.assertNotIn(selected, excluded)
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", selected))

        source = script.read_text(encoding="utf-8")
        self.assertIn('local_http_port=$(select_free_port "$local_pg_port")', source)
        self.assertIn('SYNCHROD_PG_PORT=$(select_free_port "$local_pg_port" "$local_http_port")', source)
        self.assertIn("export SYNCHROD_PG_PORT", source)
        self.assertIn('export SYNCHRO_TEST_URL="http://127.0.0.1:$local_http_port"', source)

    def test_release_has_one_post_package_approval_before_public_side_effects(self) -> None:
        workflow = (ROOT / ".github/workflows/release.yml").read_text(encoding="utf-8")
        package_gate = workflow.index("\n  package-gate:")
        publish = workflow.index("\n  publish:")
        self.assertLess(package_gate, publish)
        self.assertIn(
            "needs: [candidate, seal, package-server, package-android, package-apple]",
            workflow[package_gate:publish],
        )
        publish_workflow = workflow[publish:]
        self.assertIn("needs: [candidate, seal, package-gate]", publish_workflow)
        self.assertEqual(len(re.findall(r"^    environment: release$", workflow, re.MULTILINE)), 1)
        self.assertNotIn("DOPPLER_TOKEN", workflow)
        self.assertNotIn("MAVEN_CENTRAL_USERNAME", workflow[:publish])
        self.assertNotIn("GPG_PRIVATE_KEY", publish_workflow)
        for side_effect in ("uses: actions/attest@", "git push", "gh release create", "central-upload", "npm publish"):
            self.assertNotIn(side_effect, workflow[:publish])
        recheck = publish_workflow.index("- name: Recheck approved candidate and sealed identity")
        attestation = publish_workflow.index("uses: actions/attest@")
        first_tag = publish_workflow.index("- name: Create or verify immutable source tags")
        first_release = publish_workflow.index("gh release create")
        first_maven = publish_workflow.index("central-upload")
        first_npm = publish_workflow.index("npm publish")
        self.assertLess(recheck, attestation)
        self.assertLess(attestation, first_tag)
        self.assertLess(attestation, first_release)
        self.assertLess(attestation, first_maven)
        self.assertLess(attestation, first_npm)


if __name__ == "__main__":
    unittest.main()
