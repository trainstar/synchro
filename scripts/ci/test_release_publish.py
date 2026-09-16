#!/usr/bin/env python3
"""Test immutable publication state classification."""

from __future__ import annotations

import importlib.util
import unittest
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock


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

    def test_candidate_identity_accepts_exact_commit_and_version(self) -> None:
        release_publish.validate_candidate_identity(self.commit, self.version)

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

    def test_central_recovery_waits_for_unsettled_deployments(self) -> None:
        for state in ("PENDING", "VALIDATING"):
            with self.subTest(state=state):
                self.assertEqual(release_publish.central_recovery_action(state), "wait")

    def test_central_recovery_replaces_only_unpublished_settled_deployments(self) -> None:
        for state in ("VALIDATED", "FAILED"):
            with self.subTest(state=state):
                self.assertEqual(release_publish.central_recovery_action(state), "replace")

    def test_central_recovery_never_replaces_irreversible_deployments(self) -> None:
        for state in ("PUBLISHING", "PUBLISHED"):
            with self.subTest(state=state):
                self.assertEqual(release_publish.central_recovery_action(state), "continue")

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
