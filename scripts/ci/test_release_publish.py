#!/usr/bin/env python3
"""Test immutable publication state classification."""

from __future__ import annotations

import base64
import importlib.util
import hashlib
import json
import os
import re
import subprocess
import sys
import tempfile
import textwrap
import threading
import unittest
import urllib.error
import urllib.parse
import zipfile
import email.message
from datetime import datetime, timezone
from email import policy
from email.parser import BytesParser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from unittest import mock


ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location("release_publish", ROOT / "scripts/release-publish.py")
assert SPEC is not None and SPEC.loader is not None
release_publish = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(release_publish)


def release_step_command(name: str, workflow: str = "release.yml") -> str:
    lines = (ROOT / ".github/workflows" / workflow).read_text(encoding="utf-8").splitlines()
    start = lines.index(f"      - name: {name}")
    run = next(index for index in range(start, len(lines)) if lines[index] == "        run: |")
    end = next((index for index in range(run + 1, len(lines)) if lines[index] and not lines[index].startswith("          ")), len(lines))
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

    def test_recovery_requires_successful_candidate_for_dispatch_commit(self) -> None:
        command = release_step_command("Verify recovery workflow Candidate")
        successful = {
            "id": 7, "head_sha": self.commit, "event": "push", "head_branch": "master",
            "path": ".github/workflows/ci.yml", "status": "completed", "conclusion": "success", "run_attempt": 1,
        }
        with tempfile.TemporaryDirectory(prefix="synchro-dispatch-candidate-") as directory:
            tools = Path(directory) / "tools"
            tools.mkdir()
            proxy = tools / "python3"
            proxy.write_text(f"#!{sys.executable}\n" + textwrap.dedent("""
                import importlib.util
                import json
                import os
                import sys
                assert sys.argv[1] == os.path.join(os.environ["RUNNER_TEMP"], "release-publish.py")
                spec = importlib.util.spec_from_file_location("publisher", os.environ["TEST_PUBLISHER"])
                publisher = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(publisher)
                responses = json.loads(os.environ["TEST_CI_RESPONSES"])
                requests = []
                def request_json(url, token=None):
                    assert token == "fixture-token"
                    assert url == (
                        "https://api.github.com/repos/trainstar/synchro/actions/workflows/ci.yml/runs"
                        f"?branch=master&event=push&head_sha={os.environ['GITHUB_SHA']}&per_page=100"
                    )
                    requests.append(url)
                    return {"workflow_runs": responses[min(len(requests), len(responses)) - 1]}
                publisher.request_json = request_json
                publisher.time.sleep = lambda seconds: None
                sys.argv = sys.argv[1:]
                try:
                    raise SystemExit(publisher.main())
                finally:
                    print(f"requests={len(requests)}", file=sys.stderr)
                """), encoding="utf-8")
            proxy.chmod(0o755)
            attempts = release_publish.CI_RUN_LOOKUP_ATTEMPTS
            for responses, accepted, requests in (
                ([[]], False, attempts),
                ([[{**successful, "head_sha": "b" * 40}]], False, attempts),
                ([[{**successful, "event": "pull_request"}]], False, attempts),
                ([[{**successful, "head_branch": "dev"}]], False, attempts),
                ([[{**successful, "path": ".github/workflows/release.yml"}]], False, attempts),
                ([[{**successful, "status": "in_progress", "conclusion": None}]], False, attempts),
                ([[{**successful, "conclusion": "failure"}]], False, attempts),
                ([[successful]], True, 1),
                ([[], [], [successful]], True, 3),
            ):
                with self.subTest(responses=responses), tempfile.TemporaryDirectory() as runner:
                    result = subprocess.run(
                        ["bash", "-c", command],
                        env={
                            **os.environ, "PATH": str(tools) + os.pathsep + os.environ["PATH"],
                            "GITHUB_SHA": self.commit, "GITHUB_REPOSITORY": "trainstar/synchro",
                            "GH_TOKEN": "fixture-token", "RUNNER_TEMP": runner,
                            "TEST_PUBLISHER": str(ROOT / "scripts/release-publish.py"),
                            "TEST_CI_RESPONSES": json.dumps(responses),
                        },
                        capture_output=True, text=True, timeout=20, check=False,
                    )
                    self.assertEqual(result.returncode == 0, accepted, result.stderr)
                    self.assertIn(f"requests={requests}", result.stderr)
                    if accepted:
                        selected = json.loads((Path(runner) / "dispatch-ci-run.json").read_text(encoding="utf-8"))
                        self.assertEqual(selected, successful)

    def test_candidate_reuse_requires_identical_parent_tree_and_passed_candidate(self) -> None:
        command = release_step_command("Find a passed Candidate for the identical tree", "ci.yml")
        head, master, tested = "c" * 40, "a" * 40, "b" * 40
        tree, other = "1" * 40, "2" * 40
        repo = "repos/trainstar/synchro"

        def runs_path(sha: str) -> str:
            return f"{repo}/actions/workflows/ci.yml/runs?event=push&status=completed&head_sha={sha}&per_page=100"

        run = {"id": 7, "head_sha": tested, "head_branch": "dev", "event": "push", "conclusion": "success"}
        promotion = {
            f"{repo}/git/commits/{head}": {"tree": {"sha": tree}, "parents": [{"sha": master}, {"sha": tested}]},
            f"{repo}/git/commits/{master}": {"tree": {"sha": other}},
            f"{repo}/git/commits/{tested}": {"tree": {"sha": tree}},
            runs_path(tested): {"workflow_runs": [run]},
            f"{repo}/actions/runs/7/jobs?filter=latest&per_page=100": {"jobs": [{"name": "candidate", "conclusion": "success"}]},
        }
        back_merge = {**promotion, f"{repo}/git/commits/{head}": {"tree": {"sha": tree}, "parents": [{"sha": tested}, {"sha": master}]}}
        cases = (
            ("promotion with the tested tree", promotion, True),
            ("back-merge with the tested tree", back_merge, True),
            ("hotfix with a changed tree", {**promotion, f"{repo}/git/commits/{tested}": {"tree": {"sha": other}}}, False),
            ("failed parent run", {**promotion, runs_path(tested): {"workflow_runs": [{**run, "conclusion": "failure"}]}}, False),
            ("unprotected branch run", {**promotion, runs_path(tested): {"workflow_runs": [{**run, "head_branch": "feature"}]}}, False),
            ("pull-request run", {**promotion, runs_path(tested): {"workflow_runs": [{**run, "event": "pull_request"}]}}, False),
            ("run for another commit", {**promotion, runs_path(tested): {"workflow_runs": [{**run, "head_sha": head}]}}, False),
            ("failed candidate job", {**promotion, f"{repo}/actions/runs/7/jobs?filter=latest&per_page=100": {"jobs": [{"name": "candidate", "conclusion": "failure"}]}}, False),
        )
        with tempfile.TemporaryDirectory(prefix="synchro-candidate-reuse-") as directory:
            tools = Path(directory)
            gh = tools / "gh"
            gh.write_text(
                "#!/usr/bin/env python3\n"
                "import json, os, sys\n"
                "fixtures = json.loads(os.environ['TEST_GH_FIXTURES'])\n"
                "if sys.argv[1:2] != ['api'] or sys.argv[2] not in fixtures:\n"
                "    sys.exit(f'unexpected gh call: {sys.argv[1:]}')\n"
                "print(json.dumps(fixtures[sys.argv[2]]))\n",
                encoding="utf-8",
            )
            gh.chmod(0o755)
            for label, fixtures, reused in cases:
                with self.subTest(case=label):
                    output, summary = tools / "output", tools / "summary"
                    output.write_text("", encoding="utf-8")
                    result = subprocess.run(
                        ["bash", "-c", command],
                        env={
                            **os.environ, "PATH": str(tools) + os.pathsep + os.environ["PATH"],
                            "GITHUB_SHA": head, "GITHUB_REPOSITORY": "trainstar/synchro",
                            "GITHUB_OUTPUT": str(output), "GITHUB_STEP_SUMMARY": str(summary),
                            "TEST_GH_FIXTURES": json.dumps(fixtures),
                        },
                        capture_output=True, text=True, timeout=5, check=False,
                    )
                    self.assertEqual(result.returncode, 0, result.stderr)
                    self.assertEqual(output.read_text(encoding="utf-8"), f"reuse={str(reused).lower()}\n")

    def test_candidate_gate_accepts_skipped_suites_only_with_verified_reuse(self) -> None:
        template = release_step_command("Require every candidate dependency", "ci.yml")
        suites = ("pgrx-runtime", "server", "swift", "kotlin", "rn-ios", "rn-android", "docs")
        full = {"source-quality": "success", "codeql": "success", "dependency-scan": "success", "candidate-reuse": "success"}
        full |= {f"candidate-{suite}": "success" for suite in suites}
        reused = full | {f"candidate-{suite}": "skipped" for suite in suites}
        cases = (
            ("full run", full, "false", True),
            ("skipped suites without reuse", reused, "false", False),
            ("verified reuse", reused, "true", True),
            ("reuse with a failed suite", reused | {"candidate-rn-ios": "failure"}, "true", False),
            ("reuse with a skipped security scan", reused | {"codeql": "skipped"}, "true", False),
            ("failed reuse lookup", full | {"candidate-reuse": "failure"}, "", False),
        )
        for label, results, reuse, accepted in cases:
            with self.subTest(case=label):
                command = template.replace("${{ needs.candidate-reuse.outputs.reuse }}", reuse)
                command = re.sub(r"\$\{\{ needs\.([a-z0-9-]+)\.result \}\}", lambda match: results[match.group(1)], command)
                self.assertNotIn("${{", command)
                result = subprocess.run(["bash", "-ec", command], capture_output=True, text=True, timeout=5, check=False)
                self.assertEqual(result.returncode == 0, accepted, result.stderr)

    def test_jobs_below_conditional_jobs_check_each_dependency(self) -> None:
        # An implicit success() skips a job when any ancestor is skipped, so recovery
        # mode would silently skip Package and publish below the skipped build jobs.
        lines = (ROOT / ".github/workflows/release.yml").read_text(encoding="utf-8").splitlines()
        jobs: dict[str, dict[str, object]] = {}
        current = ""
        for line in lines[lines.index("jobs:") + 1:]:
            header = re.fullmatch(r"  ([a-z0-9-]+):", line)
            if header:
                current = header.group(1)
                jobs[current] = {"if": "", "needs": [], "body": ""}
            elif line.startswith("    if: "):
                jobs[current]["if"] = line.removeprefix("    if: ")
            elif line.startswith("    needs: "):
                jobs[current]["needs"] = [need.strip() for need in line.removeprefix("    needs: ").strip("[]").split(",")]
            else:
                jobs[current]["body"] += line + "\n"

        def ancestors(name: str) -> set[str]:
            found: set[str] = set()
            for need in jobs[name]["needs"]:
                found |= {need} | ancestors(need)
            return found

        guarded = [name for name in jobs if any(jobs[ancestor]["if"] for ancestor in ancestors(name))]
        self.assertTrue({"seal", "package-server", "package-android", "package-apple", "package-gate", "publish"} <= set(guarded))
        for name in guarded:
            job = jobs[name]
            with self.subTest(job=name):
                self.assertRegex(job["if"], r"^(always\(\)|\$\{\{ (always\(\)|!cancelled\(\)) && .+ \}\})$")
                for need in job["needs"]:
                    self.assertTrue(
                        f"needs.{need}.result == 'success'" in job["if"]
                        or f'test "${{{{ needs.{need}.result }}}}" = success' in job["body"],
                        need,
                    )

    def test_recovery_preserves_dispatch_helper_without_changing_candidate(self) -> None:
        with tempfile.TemporaryDirectory(prefix="synchro-recovery-helper-") as directory:
            root = Path(directory)
            repo = root / "source"
            repo.mkdir()
            runner = root / "runner"
            runner.mkdir()
            def git(*args: str) -> str:
                return subprocess.run(
                    ["git", *args], cwd=repo, capture_output=True, text=True, check=True,
                ).stdout.strip()
            git("init", "--quiet", "--initial-branch=master")
            git("config", "user.name", "Release fixture")
            git("config", "user.email", "release@example.invalid")
            helper = repo / "scripts/release-publish.py"
            helper.parent.mkdir()
            original = 'raise RuntimeError("original defective publisher")\n'
            helper.write_text(original, encoding="utf-8")
            git("add", ".")
            git("commit", "--quiet", "-m", "Create original candidate")
            candidate = git("rev-parse", "HEAD")
            corrected = (ROOT / "scripts/release-publish.py").read_bytes()
            helper.write_bytes(corrected)
            git("commit", "--quiet", "-am", "Correct publication helper")
            dispatch = git("rev-parse", "HEAD")
            for name, checkout in (
                ("Preserve dispatch publication helper", dispatch),
                ("Load dispatch publication helper", candidate),
            ):
                with self.subTest(step=name):
                    git("checkout", "--quiet", "--detach", checkout)
                    subprocess.run(
                        ["bash", "-c", release_step_command(name)], cwd=repo,
                        env={**os.environ, "GITHUB_SHA": dispatch, "RUNNER_TEMP": str(runner)},
                        check=True, capture_output=True, text=True,
                    )
                    git("checkout", "--quiet", "--detach", candidate)
                    self.assertEqual(helper.read_text(encoding="utf-8"), original)
                    self.assertEqual(git("status", "--porcelain"), "")
                    self.assertEqual(git("rev-parse", "HEAD"), candidate)
                    preserved = runner / "release-publish.py"
                    self.assertEqual(preserved.read_bytes(), corrected)
                    subprocess.run(
                        [sys.executable, str(preserved), "validate-candidate",
                         "--source-commit", candidate, "--version", self.version],
                        cwd=repo, check=True, capture_output=True, text=True,
                    )

    @mock.patch.object(release_publish, "request_json")
    def test_authenticated_draft_lookup_paginates_without_using_published_endpoint(self, request_json: mock.Mock) -> None:
        pages = [
            [{"id": index, "tag_name": f"other-{index}"} for index in range(1, 101)],
            [{"id": 101, "tag_name": self.identity["root_tag"], "draft": True, "assets": []}],
        ]
        request_json.side_effect = pages
        draft = release_publish.github_release("trainstar/synchro", self.identity["root_tag"], "fixture-token", include_drafts=True)
        self.assertEqual(draft, pages[1][0])
        self.assertEqual(request_json.call_args_list, [
            mock.call("https://api.github.com/repos/trainstar/synchro/releases?per_page=100&page=1", "fixture-token"),
            mock.call("https://api.github.com/repos/trainstar/synchro/releases?per_page=100&page=2", "fixture-token"),
        ])

    @mock.patch.object(release_publish, "request_json", return_value=None)
    def test_published_lookup_does_not_discover_private_drafts(self, request_json: mock.Mock) -> None:
        for token in (None, "fixture-token"):
            with self.subTest(token=token):
                request_json.reset_mock()
                self.assertIsNone(release_publish.github_release("trainstar/synchro", self.identity["root_tag"], token))
                request_json.assert_called_once_with("https://api.github.com/repos/trainstar/synchro/releases/tags/v1.2.3", token)
        with self.assertRaisesRegex(release_publish.PublicationError, "requires a token"):
            release_publish.github_release("trainstar/synchro", self.identity["root_tag"], None, include_drafts=True)

    @mock.patch.object(release_publish, "request_json")
    def test_authenticated_draft_lookup_rejects_ambiguous_or_invalid_responses(self, request_json: mock.Mock) -> None:
        draft = {"id": 1, "tag_name": self.identity["root_tag"], "draft": True, "assets": []}
        for value in (
            None,
            {},
            [None],
            [{"tag_name": self.identity["root_tag"]}],
            [draft, draft],
            [draft, {**draft, "id": 2}],
        ):
            with self.subTest(value=value):
                request_json.return_value = value
                with self.assertRaises(release_publish.PublicationError):
                    release_publish.github_release("trainstar/synchro", self.identity["root_tag"], "fixture-token", include_drafts=True)

    @mock.patch.object(release_publish, "request_bytes", return_value=None)
    @mock.patch.object(release_publish, "request_json")
    def test_authenticated_observer_recovers_draft_when_published_tag_is_absent(
        self, request_json: mock.Mock, request_bytes: mock.Mock,
    ) -> None:
        def response(url: str, token: str | None = None) -> object:
            if "/git/ref/tags/" in url:
                return {"object": {"sha": self.commit}}
            if "/releases?" in url:
                self.assertEqual(token, "fixture-token")
                return [{
                    "id": 1, "tag_name": self.identity["root_tag"], "draft": True,
                    "assets": [{
                        "name": "server", "digest": "sha256:" + self.identity["github_assets"]["server"],
                        "browser_download_url": "https://github.com/trainstar/synchro/releases/download/v1.2.3/server",
                    }],
                }]
            return None
        request_json.side_effect = response
        state = release_publish.observe_public(self.identity, "trainstar/synchro", "fixture-token", include_drafts=True)
        classified = release_publish.classify_publication(self.identity, state)
        self.assertEqual(classified["github"], "draft-partial")
        self.assertEqual(classified["next_operation"], "publish-github")
        self.assertEqual(state["github"]["assets"], {"server": self.identity["github_assets"]["server"]})

    @staticmethod
    def http_error(code: int, headers: dict[str, str]) -> urllib.error.HTTPError:
        message = email.message.Message()
        for name, value in headers.items():
            message[name] = value
        return urllib.error.HTTPError("https://api.github.com/fixture", code, "fixture", message, None)

    def test_github_reads_wait_for_rate_limit_reset_then_retry(self) -> None:
        response = mock.MagicMock()
        response.__enter__.return_value.read.return_value = b"{}"
        for headers, wait in (
            ({"retry-after": "30"}, 30.0),
            ({"retry-after": "0"}, 1.0),
            ({"x-ratelimit-remaining": "0", "x-ratelimit-reset": "1060"}, 61.0),
        ):
            for code in (403, 429):
                with self.subTest(headers=headers, code=code), \
                        mock.patch.object(release_publish.time, "time", return_value=1000.0), \
                        mock.patch.object(release_publish.time, "sleep") as sleep, \
                        mock.patch.object(release_publish.urllib.request, "urlopen", side_effect=[self.http_error(code, headers), response]):
                    self.assertEqual(release_publish.request_json("https://api.github.com/fixture", "fixture-token"), {})
                    sleep.assert_called_once_with(wait)

    def test_github_reads_fail_with_limit_headers_after_bounded_wait(self) -> None:
        with mock.patch.object(release_publish.time, "sleep") as sleep, \
                mock.patch.object(release_publish.urllib.request, "urlopen", side_effect=lambda *_args, **_kwargs: (_ for _ in ()).throw(self.http_error(429, {"retry-after": "0"}))):
            with self.assertRaisesRegex(release_publish.PublicationError, "HTTP 429"):
                release_publish.request_bytes("https://api.github.com/fixture", "fixture-token")
            self.assertEqual(sum(call.args[0] for call in sleep.call_args_list), release_publish.RATE_LIMIT_WAIT_SECONDS)
        exhausted = {"x-ratelimit-remaining": "0", "x-ratelimit-reset": str(1000 + release_publish.RATE_LIMIT_WAIT_SECONDS), "retry-after": ""}
        denied = {"x-ratelimit-remaining": "42"}
        for code, headers in ((403, exhausted), (403, denied), (500, {"retry-after": "1"})):
            with self.subTest(code=code, headers=headers), \
                    mock.patch.object(release_publish.time, "time", return_value=1000.0), \
                    mock.patch.object(release_publish.time, "sleep") as sleep, \
                    mock.patch.object(release_publish.urllib.request, "urlopen", side_effect=self.http_error(code, headers)):
                with self.assertRaises(release_publish.PublicationError) as raised:
                    release_publish.request_bytes("https://api.github.com/fixture", "fixture-token")
                sleep.assert_not_called()
                message = str(raised.exception)
                self.assertIn(f"HTTP {code}", message)
                self.assertIn(f"x-ratelimit-remaining={headers.get('x-ratelimit-remaining')}", message)
                self.assertIn(f"x-ratelimit-reset={headers.get('x-ratelimit-reset')}", message)
                self.assertIn("retry-after=", message)

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

    def test_release_workflow_creates_and_recovers_github_drafts(self) -> None:
        command = release_step_command("Create or recover draft and publish GitHub assets")
        for expression, value in (
            ("release_dir_name", "fixture"), ("version", self.version), ("source_commit", self.commit),
        ):
            command = command.replace("${{ needs.candidate.outputs." + expression + " }}", value)
        self.assertNotIn("${{", command)
        with tempfile.TemporaryDirectory(prefix="synchro-github-draft-") as directory:
            root = Path(directory)
            release = root / "dist/releases/fixture"
            (release / "artifacts").mkdir(parents=True)
            payloads = {
                "release-manifest.json": b"sealed manifest",
                "SHA256SUMS": b"sealed checksums",
                "sbom.spdx.json": b"sealed SBOM",
                "server.tar.gz": b"sealed server",
            }
            for name, data in payloads.items():
                (release / ("artifacts" if name == "server.tar.gz" else "") / name).write_bytes(data)
            tools = root / "tools"
            tools.mkdir()
            proxy = tools / "python3"
            proxy.write_text(f"#!{sys.executable}\n" + textwrap.dedent("""
                import importlib.util
                import json
                import os
                import sys
                from pathlib import Path
                assert sys.argv[1] == os.path.join(os.environ["RUNNER_TEMP"], "release-publish.py")
                spec = importlib.util.spec_from_file_location("publisher", os.environ["TEST_PUBLISHER"])
                publisher = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(publisher)
                def request_json(url, token=None):
                    assert token == "fixture-token"
                    assert url == "https://api.github.com/repos/trainstar/synchro/releases?per_page=100&page=1"
                    value = json.loads(Path(os.environ["TEST_GITHUB_STATE"]).read_text())["release"]
                    return [value] if value is not None and value["listed"] else []
                publisher.request_json = request_json
                sys.argv = sys.argv[1:]
                raise SystemExit(publisher.main())
                """), encoding="utf-8")
            proxy.chmod(0o755)
            git = tools / "git"
            git.write_text(
                '#!/bin/sh\n'
                'test "$*" = "ls-remote --tags origin refs/tags/v1.2.3" || exit 1\n'
                'printf "%s\\trefs/tags/v1.2.3\\n" "$TEST_TAG_COMMIT"\n',
                encoding="utf-8",
            )
            git.chmod(0o755)
            gh = tools / "gh"
            gh.write_text(f"#!{sys.executable}\n" + textwrap.dedent("""
                import json
                import os
                import sys
                from pathlib import Path
                path = Path(os.environ["TEST_GITHUB_STATE"])
                state = json.loads(path.read_text())
                args = sys.argv[1:]
                assert args[0] == "api", args
                release = state["release"]
                upload = "https://uploads.github.com/repos/trainstar/synchro/releases/17/assets"
                if args[1:4] == ["--method", "POST", "repos/trainstar/synchro/releases"]:
                    assert release is None
                    assert args[4:] == ["-f", "tag_name=v1.2.3", "-F", "draft=true", "-F", "generate_release_notes=true", "-f", "make_latest=false"], args
                    release = state["release"] = {
                        "id": 17, "tag_name": "v1.2.3", "draft": True, "assets": [], "listed": False,
                        "upload_url": upload + "{?name,label}",
                    }
                    state["operations"].append("create")
                    print(json.dumps(release))
                elif args[1:3] == ["--method", "POST"] and args[3].startswith(upload + "?name="):
                    assert args[4:7] == ["-H", "Content-Type: application/octet-stream", "--input"], args
                    name = args[3].split("?name=", 1)[1]
                    assert release["draft"] and name not in state["files"]
                    state["files"][name] = Path(args[7]).read_bytes().hex()
                    release["assets"].append({"name": name, "id": 100 + len(release["assets"])})
                    state["operations"].append("upload")
                elif args[1:3] == ["-H", "Accept: application/octet-stream"]:
                    asset_id = int(args[3].rsplit("/", 1)[1])
                    assert args[3] == f"repos/trainstar/synchro/releases/assets/{asset_id}"
                    name = next(asset["name"] for asset in release["assets"] if asset["id"] == asset_id)
                    sys.stdout.buffer.write(bytes.fromhex(state["files"][name]))
                    state["operations"].append("download")
                elif args[1:] == ["--method", "PATCH", "repos/trainstar/synchro/releases/17", "-F", "draft=false", "-f", "make_latest=false"]:
                    release["draft"] = False
                    state["operations"].append("publish")
                else:
                    raise AssertionError(args)
                path.write_text(json.dumps(state))
                """), encoding="utf-8")
            gh.chmod(0o755)
            for initial, tag_commit, accepted in (
                ("absent", self.commit, True),
                ("draft-partial", self.commit, True),
                ("absent", "b" * 40, False),
            ):
                with self.subTest(initial=initial, tag_commit=tag_commit):
                    runner = root / f"{initial}-{tag_commit[0]}"
                    runner.mkdir()
                    existing = initial == "draft-partial"
                    state_path = runner / "github-state.json"
                    state_path.write_text(json.dumps({
                        "release": {
                            "id": 17, "tag_name": "v1.2.3", "draft": True, "listed": True,
                            "upload_url": "https://uploads.github.com/repos/trainstar/synchro/releases/17/assets{?name,label}",
                            "assets": [{"name": "release-manifest.json", "id": 100}],
                        } if existing else None,
                        "files": {"release-manifest.json": payloads["release-manifest.json"].hex()} if existing else {},
                        "operations": [],
                    }), encoding="utf-8")
                    (runner / "public-before-classification.json").write_text(
                        json.dumps({"github": initial}), encoding="utf-8",
                    )
                    result = subprocess.run(
                        ["bash", "-c", command], cwd=root,
                        env={
                            **os.environ,
                            "PATH": str(tools) + os.pathsep + os.environ["PATH"],
                            "RUNNER_TEMP": str(runner), "GITHUB_OUTPUT": str(runner / "outputs"),
                            "GITHUB_REPOSITORY": "trainstar/synchro", "GH_TOKEN": "fixture-token",
                            "TEST_PUBLISHER": str(ROOT / "scripts/release-publish.py"),
                            "TEST_GITHUB_STATE": str(state_path), "TEST_TAG_COMMIT": tag_commit,
                        },
                        capture_output=True, text=True, timeout=20, check=False,
                    )
                    state = json.loads(state_path.read_text(encoding="utf-8"))
                    if not accepted:
                        self.assertNotEqual(result.returncode, 0)
                        self.assertIn("release tag is not at the candidate commit", result.stderr)
                        self.assertEqual(state["operations"], [])
                        continue
                    self.assertEqual(result.returncode, 0, result.stderr)
                    self.assertEqual(state["files"], {name: data.hex() for name, data in payloads.items()})
                    self.assertFalse(state["release"]["draft"])
                    self.assertEqual(
                        state["operations"],
                        ["download", "upload", "upload", "upload", "publish"] if existing
                        else ["create", "upload", "upload", "upload", "upload", "publish"],
                    )
                    self.assertEqual(
                        json.loads((runner / "github-operation.json").read_text(encoding="utf-8")),
                        {"release_id": "17", "tag": "v1.2.3", "source_commit": self.commit, "operation": "publish-draft"},
                    )

    def write_release_fixture(self, release_dir: Path) -> dict[str, Any]:
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
        return release_publish.identity_for_directory(release_dir)

    def test_registry_credential_preflight_fails_before_tags(self) -> None:
        workflow = (ROOT / ".github/workflows/release.yml").read_text(encoding="utf-8")
        self.assertLess(
            workflow.index("      - name: Verify registry publication credentials\n"),
            workflow.index("      - name: Create or verify immutable source tags\n"),
        )
        command = release_step_command("Verify registry publication credentials")
        command = command.replace("${{ needs.candidate.outputs.release_dir_name }}", "fixture")
        self.assertNotIn("${{", command)
        central = "Bearer " + base64.b64encode(b"fixture-user:fixture-password").decode("ascii")
        exchange = "/-/npm/v1/oidc/token/exchange/package/@trainstar%2fsynchro-react-native"
        with tempfile.TemporaryDirectory(prefix="synchro-registry-preflight-") as directory:
            root = Path(directory)
            self.write_release_fixture(root / "dist/releases/fixture")
            tools = root / "tools"
            tools.mkdir()
            proxy = tools / "python3"
            proxy.write_text(f"#!{sys.executable}\n" + textwrap.dedent("""
                import importlib.util
                import os
                import sys
                assert sys.argv[1] == os.path.join(os.environ["RUNNER_TEMP"], "release-publish.py")
                spec = importlib.util.spec_from_file_location("publisher", os.environ["TEST_PUBLISHER"])
                publisher = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(publisher)
                publisher.CENTRAL_API = os.environ["TEST_SERVER_URL"]
                publisher.NPM_REGISTRY = os.environ["TEST_SERVER_URL"]
                sys.argv = sys.argv[1:]
                raise SystemExit(publisher.main())
                """), encoding="utf-8")
            proxy.chmod(0o755)
            for maven, npm, password, oidc, exchange_status, error in (
                ("absent", "absent", "fixture-password", True, 200, None),
                ("absent", "absent", "wrong-password", True, 200, "Central request failed with HTTP 401"),
                ("published", "absent", "", True, 404, "npm trusted publishing token exchange failed with HTTP 404"),
                ("published", "absent", "", False, 200, "GitHub OIDC token request is unavailable"),
                ("published", "published-latest", "", True, 200, None),
            ):
                with self.subTest(maven=maven, npm=npm, password=password, oidc=oidc, exchange_status=exchange_status):
                    requests = []

                    class Handler(BaseHTTPRequestHandler):
                        def log_message(self, *_args: object) -> None:
                            return

                        def respond(self, status: int, value: object) -> None:
                            payload = json.dumps(value).encode()
                            self.send_response(status)
                            self.send_header("Content-Length", str(len(payload)))
                            self.end_headers()
                            self.wfile.write(payload)

                        def do_GET(self) -> None:
                            parsed = urllib.parse.urlsplit(self.path)
                            requests.append(("GET", parsed.path))
                            authorization = self.headers["Authorization"]
                            if parsed.path == "/deployments":
                                if authorization != central:
                                    self.respond(401, {"error": "not authenticated"})
                                    return
                                self.respond(200, {"deployments": [], "page": 0, "pageSize": 100, "pageCount": 0, "totalResultCount": 0})
                            elif parsed.path == "/oidc":
                                self.assertEqual(urllib.parse.parse_qs(parsed.query), {"run": ["1"], "audience": ["npm:registry.npmjs.org"]})
                                self.assertEqual(authorization, "Bearer request-token-secret")
                                self.respond(200, {"value": "id-token-secret"})
                            else:
                                self.respond(400, {})

                        def do_POST(self) -> None:
                            requests.append(("POST", self.path))
                            if self.path != exchange or self.headers["Authorization"] != "Bearer id-token-secret":
                                self.respond(400, {})
                                return
                            self.respond(exchange_status, {"token": "npm-token-secret"} if exchange_status == 200 else {"message": "no trusted publisher"})

                        assertEqual = self.assertEqual

                    runner = root / f"{maven}-{npm}-{password}-{oidc}-{exchange_status}"
                    runner.mkdir()
                    (runner / "public-before-classification.json").write_text(
                        json.dumps({"maven": maven, "npm": npm}), encoding="utf-8",
                    )
                    with ThreadingHTTPServer(("127.0.0.1", 0), Handler) as server:
                        thread = threading.Thread(target=server.serve_forever)
                        thread.start()
                        url = f"http://127.0.0.1:{server.server_port}"
                        environment = {
                            **os.environ,
                            "PATH": str(tools) + os.pathsep + os.environ["PATH"],
                            "RUNNER_TEMP": str(runner),
                            "MAVEN_CENTRAL_USERNAME": "fixture-user" if password else "",
                            "MAVEN_CENTRAL_PASSWORD": password,
                            "TEST_PUBLISHER": str(ROOT / "scripts/release-publish.py"),
                            "TEST_SERVER_URL": url,
                        }
                        environment.pop("ACTIONS_ID_TOKEN_REQUEST_URL", None)
                        environment.pop("ACTIONS_ID_TOKEN_REQUEST_TOKEN", None)
                        if oidc:
                            environment["ACTIONS_ID_TOKEN_REQUEST_URL"] = f"{url}/oidc?run=1"
                            environment["ACTIONS_ID_TOKEN_REQUEST_TOKEN"] = "request-token-secret"
                        try:
                            result = subprocess.run(
                                ["bash", "-c", command], cwd=root, env=environment,
                                capture_output=True, text=True, timeout=20, check=False,
                            )
                        finally:
                            server.shutdown()
                            thread.join()
                    output = result.stdout + result.stderr
                    for secret in ("request-token-secret", "id-token-secret", "npm-token-secret", "fixture-password"):
                        self.assertNotIn(secret, output)
                    if error is None:
                        self.assertEqual(result.returncode, 0, result.stderr)
                    else:
                        self.assertNotEqual(result.returncode, 0)
                        self.assertIn(error, result.stderr)
                    expected = []
                    if maven != "published":
                        expected.append(("GET", "/deployments"))
                    if npm == "absent" and oidc and password != "wrong-password":
                        expected.extend([("GET", "/oidc"), ("POST", exchange)])
                    self.assertEqual(requests, expected)

    def test_registry_polls_do_not_spend_github_api_requests(self) -> None:
        command = release_step_command("Publish and verify Maven deployment")
        command = command.replace("${{ needs.candidate.outputs.release_dir_name }}", "fixture")
        self.assertNotIn("${{", command)
        with tempfile.TemporaryDirectory(prefix="synchro-registry-poll-") as directory:
            root = Path(directory)
            self.write_release_fixture(root / "dist/releases/fixture")
            tools = root / "tools"
            tools.mkdir()
            proxy = tools / "python3"
            proxy.write_text(f"#!{sys.executable}\n" + textwrap.dedent("""
                import importlib.util
                import os
                import sys
                from pathlib import Path
                assert sys.argv[1] == os.path.join(os.environ["RUNNER_TEMP"], "release-publish.py")
                spec = importlib.util.spec_from_file_location("publisher", os.environ["TEST_PUBLISHER"])
                publisher = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(publisher)
                log = Path(os.environ["TEST_REQUEST_LOG"])
                def request_bytes(url, token=None):
                    assert "api.github.com" not in url, url
                    with log.open("a") as stream:
                        stream.write(url + "\\n")
                    if len(log.read_text().splitlines()) < 3:
                        return None
                    return b"sealed Maven payload"
                publisher.request_bytes = request_bytes
                sys.argv = sys.argv[1:]
                raise SystemExit(publisher.main())
                """), encoding="utf-8")
            proxy.chmod(0o755)
            sleeper = tools / "sleep"
            sleeper.write_text('#!/bin/sh\nprintf "%s\\n" "$1" >> "$TEST_SLEEP_LOG"\n', encoding="utf-8")
            sleeper.chmod(0o755)
            request_log = root / "requests.log"
            sleep_log = root / "sleep.log"
            result = subprocess.run(
                ["bash", "-c", command], cwd=root,
                env={
                    **os.environ,
                    "PATH": str(tools) + os.pathsep + os.environ["PATH"],
                    "GITHUB_WORKSPACE": str(root), "RUNNER_TEMP": str(root),
                    "DEPLOYMENT_ID": "", "DEPLOYMENT_STATE": "PUBLIC",
                    "TEST_PUBLISHER": str(ROOT / "scripts/release-publish.py"),
                    "TEST_REQUEST_LOG": str(request_log), "TEST_SLEEP_LOG": str(sleep_log),
                },
                capture_output=True, text=True, timeout=20, check=False,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(
                request_log.read_text(encoding="utf-8").splitlines(),
                [f"{release_publish.MAVEN_BASE}/fit/trainstar/synchro/1.2.3/synchro-1.2.3.aar"] * 3,
            )
            self.assertEqual(sleep_log.read_text(encoding="utf-8").splitlines(), ["15", "15"])

    def test_release_workflow_executes_private_recovery_effects(self) -> None:
        command = release_step_command("Resolve or upload and validate Maven deployment")
        command = command.replace("${{ needs.candidate.outputs.release_dir_name }}", "fixture")
        self.assertNotIn("${{", command)

        with tempfile.TemporaryDirectory(prefix="synchro-publication-effects-") as directory:
            root = Path(directory)
            release_dir = root / "dist/releases/fixture"
            identity = self.write_release_fixture(release_dir)
            bundle = (release_dir / "maven.zip").read_bytes()
            name = identity["maven_bundle"]["deployment_name"]
            tools = root / "tools"
            tools.mkdir()
            python_proxy = tools / "python3"
            python_proxy.write_text(f"#!{sys.executable}\n" + textwrap.dedent("""
                import importlib.util
                import os
                import sys
                assert sys.argv[1] == os.path.join(os.environ["RUNNER_TEMP"], "release-publish.py")
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
