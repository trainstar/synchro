#!/usr/bin/env python3
"""Test immutable publication state classification."""

from __future__ import annotations

import importlib.util
import os
import subprocess
import sys
import tempfile
import time
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

    def run_fixture_scenario(
        self,
        root: Path,
        scenario: str,
    ) -> tuple[subprocess.CompletedProcess[str], dict[str, Path]]:
        fake_bin = root / "bin"
        fake_bin.mkdir()
        ssh_count = root / "ssh-count"
        ssh_log = root / "ssh-log"
        lifecycle_log = root / "lifecycle-log"
        pidfd_log = root / "pidfd-log"
        python_argv_log = root / "python-argv-log"
        provisioner_argv_log = root / "provisioner-argv-log"
        fake_python = fake_bin / "python3"
        fake_python.write_text(
            """#!/bin/sh
set -eu
printf '%s\n' "$*" >> "$FAKE_PYTHON_ARGV_LOG"
if [ "${1:-}" = - ]; then
  case "${2:-}" in
    record)
      cat >/dev/null
      printf '%s %s\n' "$3" 123456 > "$4"
      chmod 600 "$4"
      printf '%s\n' "record:$4" >> "$FAKE_PIDFD_LOG"
      exit 0
      ;;
    wait)
      cat >/dev/null
      printf '%s\n' "wait:$3" >> "$FAKE_PIDFD_LOG"
      case "$FAKE_FIXTURE_SCENARIO" in
        password-argv) exit 0 ;;
        pidfd-identity)
          printf '%s\n' "pidfd helper: process executable does not match the owned process" >&2
          ;;
        *)
          printf '%s\n' "provisioner exited before attach environment became available" >&2
          ;;
      esac
      tail -c 16384 "$6" >&2
      exit 1
      ;;
    stop)
      cat >/dev/null
      printf '%s\n' "stop:$3" >> "$FAKE_PIDFD_LOG"
      if [ "$FAKE_FIXTURE_SCENARIO" = pidfd-identity ]; then
        printf '%s\n' "pidfd helper: process executable does not match the owned process" >&2
        exit 1
      fi
      if [ ! -f "$3" ] && [ "$6" = required ]; then
        printf '%s\n' "pidfd helper: process identity is missing: $3" >&2
        exit 1
      fi
      if [ -f "$3" ]; then
        pid=$(cut -d ' ' -f 1 "$3")
        kill "$pid" >/dev/null 2>&1 || true
      fi
      printf '%s\n' "signal:$3" >> "$FAKE_PIDFD_LOG"
      exit 0
      ;;
  esac
fi
exec "$REAL_PYTHON" "$@"
""",
            encoding="utf-8",
        )
        fake_python.chmod(0o700)
        fake_ssh = fake_bin / "ssh"
        fake_ssh.write_text(
            """#!/bin/sh
set -eu
while [ "$#" -gt 0 ]; do
  case "$1" in
    -i|-o|-L) shift 2 ;;
    -N) shift ;;
    *) break ;;
  esac
done
[ "$#" -gt 0 ]
shift
printf '%s\n' "$*" >> "$FAKE_SSH_LOG"
count=0
[ ! -f "$FAKE_SSH_COUNT" ] || count=$(cat "$FAKE_SSH_COUNT")
count=$((count + 1))
printf '%s\n' "$count" > "$FAKE_SSH_COUNT"
case "$count" in
  1) "$@" ;;
  2)
    case "$FAKE_FIXTURE_SCENARIO" in
      pre-launch)
        cat >/dev/null
        exit 41
        ;;
      *) "$@" ;;
    esac
    ;;
  3)
    if [ "$FAKE_FIXTURE_SCENARIO" = pre-launch ]; then
      "$@"
      exit 0
    fi
    if [ "$FAKE_FIXTURE_SCENARIO" = launch-race ]; then
      cat >/dev/null
      exit 41
    fi
    cat >/dev/null
    after_separator=0
    run=
    for argument in "$@"; do
      if [ "$after_separator" -eq 1 ]; then
        run=$argument
        break
      fi
      [ "$argument" = -- ] && after_separator=1
    done
    [ -n "$run" ]
    cat > "$run/provisioner" <<'PROVISIONER'
#!/bin/sh
set -eu
case "$1" in
  lifecycle)
    [ "$2" = --state-dir ] && [ "$4" = destroy ]
    printf '%s\n' "$*" >> "$FAKE_LIFECYCLE_LOG"
    ;;
  prepare)
    printf '%s\n' "$*" >> "$FAKE_PROVISIONER_ARGV_LOG"
    ;;
  *) exit 97 ;;
esac
PROVISIONER
    chmod 700 "$run/provisioner"
    cat > "$run/synchrod-pg" <<'ADAPTER'
#!/bin/sh
exec sleep 300
ADAPTER
    chmod 700 "$run/synchrod-pg"
    printf '%s\n' "99999999 123456" > "$run/provisioner.pid"
    chmod 600 "$run/provisioner.pid"
    {
      printf '%16384s' '' | tr ' ' x
      printf '%s\n' "fixture provisioner failed before attach"
    } > "$run/provisioner.log"
    case "$FAKE_FIXTURE_SCENARIO" in
      lifecycle-cleanup|password-argv)
        mkdir -m 700 "$run/state"
        printf '%s\n' '{"run_id":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","control_address":"127.0.0.1:54321","destroyed":false}' > "$run/state/lifecycle-state.json"
        chmod 600 "$run/state/lifecycle-state.json"
        ;;
    esac
    if [ "$FAKE_FIXTURE_SCENARIO" = password-argv ]; then
      printf '%s\n' "admin-secret-value" > "$run/state/admin-password"
      printf '%s\n' "adapter-secret-value" > "$run/state/adapter-password"
      printf '%s\n' "observer-secret-value" > "$run/state/observer-password"
      printf '%s\n' "worker-secret-value" > "$run/state/worker-password"
      printf '%s\n' "operator-secret-value" > "$run/state/operator-password"
      printf '%s\n' "jwt-secret-value" > "$run/state/jwt-secret"
      chmod 600 "$run/state/admin-password" "$run/state/adapter-password" \
        "$run/state/observer-password" "$run/state/worker-password" \
        "$run/state/operator-password" "$run/state/jwt-secret"
      cat > "$run/attach.env" <<EOF
SYNCHRO_CONFORMANCE_ATTACH_DATABASE_URL='postgres://127.0.0.1:54321/fixture?sslmode=disable'
SYNCHRO_CONFORMANCE_ADMIN_USER='admin'
SYNCHRO_CONFORMANCE_ADMIN_PASSWORD_FILE='$run/state/admin-password'
SYNCHRO_CONFORMANCE_ADAPTER_USER='adapter'
SYNCHRO_CONFORMANCE_ADAPTER_PASSWORD_FILE='$run/state/adapter-password'
SYNCHRO_CONFORMANCE_JWT_SECRET_FILE='$run/state/jwt-secret'
SYNCHRO_CONFORMANCE_ATTACH_RUN_ID='bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'
EOF
      chmod 600 "$run/attach.env"
      printf '%s\n' 54322 > "$run/http.port"
    fi
    ;;
  *)
    if [ "$#" -eq 0 ]; then
      exec sleep 5
    fi
    "$@"
    ;;
esac
""",
            encoding="utf-8",
        )
        fake_ssh.chmod(0o700)
        fake_scp = fake_bin / "scp"
        fake_scp.write_text(
            """#!/bin/sh
set -eu
while [ "$#" -gt 0 ]; do
  case "$1" in
    -i|-o) shift 2 ;;
    *) break ;;
  esac
done
[ "$#" -eq 2 ]
source=${1#*:}
cp "$source" "$2"
""",
            encoding="utf-8",
        )
        fake_scp.chmod(0o700)
        fake_curl = fake_bin / "curl"
        fake_curl.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
        fake_curl.chmod(0o700)

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
        remote_root = (root / "remote").resolve()
        environment = os.environ.copy()
        environment.update(
            {
                "PATH": f"{fake_bin}:{environment['PATH']}",
                "REAL_PYTHON": sys.executable,
                "FAKE_SSH_COUNT": str(ssh_count),
                "FAKE_SSH_LOG": str(ssh_log),
                "FAKE_FIXTURE_SCENARIO": scenario,
                "FAKE_LIFECYCLE_LOG": str(lifecycle_log),
                "FAKE_PIDFD_LOG": str(pidfd_log),
                "FAKE_PYTHON_ARGV_LOG": str(python_argv_log),
                "FAKE_PROVISIONER_ARGV_LOG": str(provisioner_argv_log),
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
                str(remote_root),
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
        return result, {
            "remote_run": remote_root / "run-123-1-candidate-swift-CI-SWIFT",
            "ssh_log": ssh_log,
            "lifecycle_log": lifecycle_log,
            "pidfd_log": pidfd_log,
            "python_argv_log": python_argv_log,
            "provisioner_argv_log": provisioner_argv_log,
        }

    def test_pre_launch_cleanup_removes_unlaunched_run(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            result, paths = self.run_fixture_scenario(Path(directory), "pre-launch")
            self.assertEqual(result.returncode, 41)
            self.assertFalse(paths["remote_run"].exists())
            self.assertIn("pre-launch", paths["ssh_log"].read_text(encoding="utf-8"))

    def test_launch_cleanup_race_retains_run(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            result, paths = self.run_fixture_scenario(Path(directory), "launch-race")
            self.assertEqual(result.returncode, 41)
            self.assertTrue(paths["remote_run"].is_dir())
            self.assertIn("pre-attach", paths["ssh_log"].read_text(encoding="utf-8"))
            self.assertIn("process identity is missing", result.stderr)
            self.assertIn("retained " + str(paths["remote_run"]), result.stderr)

    def test_pidfd_identity_rejection_does_not_signal(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            result, paths = self.run_fixture_scenario(Path(directory), "pidfd-identity")
            self.assertEqual(result.returncode, 1)
            self.assertTrue(paths["remote_run"].is_dir())
            self.assertIn("process executable does not match", result.stderr)
            pidfd_events = paths["pidfd_log"].read_text(encoding="utf-8")
            self.assertIn("wait:", pidfd_events)
            self.assertIn("stop:", pidfd_events)
            self.assertNotIn("signal:", pidfd_events)

    def test_lifecycle_cleanup_restores_and_removes_run(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            result, paths = self.run_fixture_scenario(Path(directory), "lifecycle-cleanup")
            self.assertEqual(result.returncode, 1)
            self.assertFalse(paths["remote_run"].exists())
            self.assertIn(
                "lifecycle --state-dir "
                + str(paths["remote_run"] / "state")
                + " destroy bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                paths["lifecycle_log"].read_text(encoding="utf-8"),
            )

    def test_absent_lifecycle_state_stops_and_retains(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            result, paths = self.run_fixture_scenario(Path(directory), "absent-state")
            self.assertEqual(result.returncode, 1)
            self.assertTrue(paths["remote_run"].is_dir())
            self.assertIn("pre-attach cleanup lacks usable lifecycle state", result.stderr)
            pidfd_events = paths["pidfd_log"].read_text(encoding="utf-8")
            self.assertIn("stop:", pidfd_events)
            self.assertIn("signal:", pidfd_events)

    def test_password_values_never_enter_process_argv(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            result, paths = self.run_fixture_scenario(Path(directory), "password-argv")
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertFalse(paths["remote_run"].exists())
            python_argv = paths["python_argv_log"].read_text(encoding="utf-8")
            provisioner_argv = paths["provisioner_argv_log"].read_text(encoding="utf-8")
            self.assertNotIn("admin-secret-value", python_argv)
            self.assertNotIn("adapter-secret-value", python_argv)
            self.assertNotIn("admin-secret-value", provisioner_argv)
            self.assertNotIn("--database-url", provisioner_argv)

    def test_attach_wait_fails_immediately_when_provisioner_exits(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            started = time.monotonic()
            result, paths = self.run_fixture_scenario(Path(directory), "early-exit")
            duration = time.monotonic() - started
            self.assertEqual(result.returncode, 1)
            self.assertLess(duration, 10)
            self.assertIn("provisioner exited before attach environment became available", result.stderr)
            self.assertIn("fixture provisioner failed before attach", result.stderr)
            self.assertLess(len(result.stderr.encode("utf-8")), 18000)
            self.assertTrue(paths["remote_run"].is_dir())


if __name__ == "__main__":
    unittest.main()
