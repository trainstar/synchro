#!/usr/bin/env python3
"""Inspect immutable release identities and classify resumable publication state."""

from __future__ import annotations

import argparse
import base64
import datetime
import hashlib
import json
import os
import re
import sys
import tempfile
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from pathlib import Path
from typing import Any


SHA256 = re.compile(r"^[0-9a-f]{64}$")
COMMIT = re.compile(r"^[0-9a-f]{40}$")
VERSION = re.compile(r"^[0-9]+\.[0-9]+\.[0-9]+$")
SLSA_PROVENANCE = re.compile(r"^https://slsa[.]dev/provenance/v[0-9]+(?:[.][0-9]+)?$")
MAVEN_STATES = {"PENDING", "VALIDATING", "VALIDATED", "PUBLISHING", "PUBLISHED", "FAILED"}
NPM_PACKAGE = "@trainstar/synchro-react-native"
MAVEN_BASE = "https://repo1.maven.org/maven2"
CENTRAL_API = "https://central.sonatype.com/api/v1/publisher"


class PublicationError(ValueError):
    """Describe one fail-closed publication identity error."""


def load_json(path: Path, label: str) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise PublicationError(f"{label} is missing or malformed: {error}") from error


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            json.dump(value, stream, indent=2, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError as error:
        raise PublicationError(f"cannot hash {path}: {error}") from error
    return digest.hexdigest()


def normalize_sha256(value: Any, label: str) -> str:
    if not isinstance(value, str):
        raise PublicationError(f"{label} is invalid")
    digest = value.removeprefix("sha256:")
    if not SHA256.fullmatch(digest):
        raise PublicationError(f"{label} is invalid")
    return digest


def positive_identifier(value: Any, label: str) -> str:
    if isinstance(value, bool):
        raise PublicationError(f"{label} is invalid")
    text = str(value)
    if not re.fullmatch(r"[1-9][0-9]*", text):
        raise PublicationError(f"{label} is invalid")
    return text


def validate_candidate_identity(source_commit: str, version: str) -> None:
    if not COMMIT.fullmatch(source_commit):
        raise PublicationError("candidate source commit is invalid")
    if not VERSION.fullmatch(version):
        raise PublicationError("candidate version is invalid")


def verify_sealed_receipt(
    receipt: Any,
    artifact: Any,
    manifest: Any,
    expected_run_id: str,
    *,
    now: datetime.datetime | None = None,
) -> dict[str, str]:
    expected_receipt = {"artifact_id", "artifact_digest", "run_id", "run_attempt", "expires_at"}
    if not isinstance(receipt, dict) or set(receipt) != expected_receipt:
        raise PublicationError("sealed artifact receipt has invalid members")
    run_id = positive_identifier(receipt["run_id"], "sealed artifact receipt run ID")
    if run_id != positive_identifier(expected_run_id, "expected recovery run ID"):
        raise PublicationError("sealed artifact receipt belongs to another workflow run")
    run_attempt = positive_identifier(receipt["run_attempt"], "sealed artifact receipt run attempt")
    artifact_id = positive_identifier(receipt["artifact_id"], "sealed artifact receipt artifact ID")
    receipt_digest = normalize_sha256(receipt["artifact_digest"], "sealed artifact receipt digest")
    expires_at = receipt["expires_at"]
    if not isinstance(expires_at, str) or not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z", expires_at):
        raise PublicationError("sealed artifact receipt expiration is invalid")
    try:
        expiration = datetime.datetime.strptime(expires_at, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=datetime.timezone.utc)
    except ValueError as error:
        raise PublicationError("sealed artifact receipt expiration is invalid") from error
    current = now or datetime.datetime.now(datetime.timezone.utc)
    if current.tzinfo is None:
        raise PublicationError("receipt verification time must include a time zone")
    if expiration <= current:
        raise PublicationError("sealed artifact receipt has expired")

    if not isinstance(artifact, dict):
        raise PublicationError("sealed artifact API response is invalid")
    if positive_identifier(artifact.get("id"), "sealed artifact API ID") != artifact_id:
        raise PublicationError("sealed artifact API identity differs from the receipt")
    if artifact.get("name") != "sealed-release" or artifact.get("expired") is not False:
        raise PublicationError("sealed artifact is missing or expired")
    workflow_run = artifact.get("workflow_run")
    if not isinstance(workflow_run, dict) or positive_identifier(workflow_run.get("id"), "sealed artifact workflow run ID") != run_id:
        raise PublicationError("sealed artifact belongs to another workflow run")
    api_digest = normalize_sha256(artifact.get("digest"), "sealed artifact API digest")
    if api_digest != receipt_digest:
        raise PublicationError("sealed artifact digest differs from the receipt")

    if not isinstance(manifest, dict):
        raise PublicationError("sealed release manifest is invalid")
    build = manifest.get("build")
    if not isinstance(build, dict):
        raise PublicationError("sealed release manifest build identity is invalid")
    if positive_identifier(build.get("run_id"), "sealed release build run ID") != run_id:
        raise PublicationError("sealed release was produced by another workflow run")
    if positive_identifier(build.get("run_attempt"), "sealed release build run attempt") != run_attempt:
        raise PublicationError("sealed release was produced by another workflow attempt")
    if build.get("workflow_path") != ".github/workflows/release.yml":
        raise PublicationError("sealed release was produced by another workflow")
    return {
        "artifact_id": artifact_id,
        "artifact_digest": receipt_digest,
        "run_id": run_id,
        "run_attempt": run_attempt,
        "expires_at": expires_at,
    }


def release_identity(release_dir: Path) -> dict[str, Any]:
    manifest = load_json(release_dir / "release-manifest.json", "release manifest")
    if not isinstance(manifest, dict) or manifest.get("schema_version") != 1:
        raise PublicationError("release manifest has an unsupported shape")
    version = manifest.get("release_version")
    source = manifest.get("source")
    distributions = manifest.get("distributions")
    if not isinstance(version, str):
        raise PublicationError("release manifest version is invalid")
    if not isinstance(source, dict):
        raise PublicationError("release manifest source commit is invalid")
    validate_candidate_identity(str(source.get("commit", "")), version)
    if source.get("source_tags") != [f"api/go/v{version}", f"v{version}"]:
        raise PublicationError("release manifest source tags are invalid")
    if not isinstance(distributions, list):
        raise PublicationError("release manifest distributions are invalid")

    roles: dict[str, dict[str, Any]] = {}
    payloads: dict[str, str] = {}
    for record in distributions:
        if not isinstance(record, dict) or not isinstance(record.get("role"), str):
            raise PublicationError("release manifest contains a malformed distribution")
        role = record["role"]
        if role in roles:
            raise PublicationError(f"release manifest repeats role {role}")
        roles[role] = record
        if record.get("kind") == "file":
            relative = record.get("path")
            expected_hash = record.get("sha256")
            if not isinstance(relative, str) or not SHA256.fullmatch(str(expected_hash)):
                raise PublicationError(f"release manifest file identity is invalid for {role}")
            path = release_dir / relative
            if not path.is_file() or path.is_symlink() or sha256_file(path) != expected_hash:
                raise PublicationError(f"sealed payload identity is invalid: {relative}")
            payloads[relative] = expected_hash
    required_roles = {"pg-extension", "adapter", "seed-tool", "kotlin-maven", "react-native-npm"}
    if not required_roles.issubset(roles):
        raise PublicationError("release manifest lacks the five public file payloads")
    required_paths = {str(roles[role].get("path", "")) for role in required_roles}
    if set(payloads) != required_paths:
        raise PublicationError("release manifest lacks the five public file payloads")

    controls: dict[str, str] = {}
    for name in ("release-manifest.json", "SHA256SUMS", "sbom.spdx.json"):
        path = release_dir / name
        if not path.is_file() or path.is_symlink():
            raise PublicationError(f"sealed release control file is missing: {name}")
        controls[name] = sha256_file(path)
    github_assets = {Path(name).name: digest for name, digest in {**payloads, **controls}.items()}
    if len(github_assets) != len(payloads) + len(controls):
        raise PublicationError("GitHub release asset names are not unique")
    maven_record = roles["kotlin-maven"]
    npm_record = roles["react-native-npm"]
    return {
        "version": version,
        "source_commit": source["commit"],
        "root_tag": f"v{version}",
        "go_tag": f"api/go/v{version}",
        "candidate_id": manifest.get("candidate_id"),
        "github_assets": github_assets,
        "maven_bundle": {
            "path": maven_record["path"],
            "sha256": maven_record["sha256"],
            "deployment_name": f"synchro-{version}-{source['commit']}",
        },
        "npm": {"path": npm_record["path"], "sha256": npm_record["sha256"], "package": NPM_PACKAGE},
    }


def maven_entries(path: Path) -> dict[str, str]:
    try:
        with zipfile.ZipFile(path) as archive:
            result: dict[str, str] = {}
            for info in archive.infolist():
                if info.is_dir() or info.filename in result:
                    raise PublicationError("Maven archive contains a directory or duplicate entry")
                result[info.filename] = sha256_bytes(archive.read(info))
    except (OSError, zipfile.BadZipFile) as error:
        raise PublicationError(f"Maven archive is invalid: {error}") from error
    if not result:
        raise PublicationError("Maven archive is empty")
    return result


def require_hash_map(actual: Any, expected: dict[str, str], label: str, *, partial: bool) -> bool:
    if not isinstance(actual, dict):
        raise PublicationError(f"{label} identity map is invalid")
    unexpected = set(actual).difference(expected)
    if unexpected:
        raise PublicationError(f"{label} contains unexpected identities: {sorted(unexpected)}")
    for name, digest in actual.items():
        if digest != expected[name]:
            raise PublicationError(f"{label} byte identity differs: {name}")
    if not partial and actual and set(actual) != set(expected):
        raise PublicationError(f"{label} is incomplete")
    return set(actual) == set(expected)


def has_npm_provenance(distribution: Any) -> bool:
    if not isinstance(distribution, dict):
        return False
    attestations = distribution.get("attestations")
    provenance = attestations.get("provenance") if isinstance(attestations, dict) else None
    attestation_url = attestations.get("url") if isinstance(attestations, dict) else None
    parsed_url = urllib.parse.urlsplit(attestation_url) if isinstance(attestation_url, str) else None
    return (
        isinstance(provenance, dict)
        and isinstance(provenance.get("predicateType"), str)
        and SLSA_PROVENANCE.fullmatch(provenance["predicateType"]) is not None
        and parsed_url is not None
        and parsed_url.scheme == "https"
        and parsed_url.netloc == "registry.npmjs.org"
        and parsed_url.path.startswith("/-/npm/v1/attestations/")
        and not parsed_url.query
        and not parsed_url.fragment
    )


def classify_publication(identity: dict[str, Any], state: Any) -> dict[str, Any]:
    if not isinstance(state, dict) or set(state) != {"tags", "github", "maven", "npm"}:
        raise PublicationError("publication state has invalid members")
    tags = state["tags"]
    if not isinstance(tags, dict) or set(tags) != {identity["root_tag"], identity["go_tag"]}:
        raise PublicationError("source tag state is invalid")
    present_tags = []
    for tag, commit in tags.items():
        if commit is not None:
            if commit != identity["source_commit"]:
                raise PublicationError(f"source tag points to a different commit: {tag}")
            present_tags.append(tag)
    tag_status = "absent" if not present_tags else "complete" if len(present_tags) == 2 else "partial"

    github = state["github"]
    if github is None:
        github_status = "absent"
    else:
        if not isinstance(github, dict) or set(github) != {"draft", "latest", "assets"}:
            raise PublicationError("GitHub release state is invalid")
        complete = require_hash_map(github["assets"], identity["github_assets"], "GitHub release", partial=True)
        if github["draft"] not in {True, False} or github["latest"] not in {True, False}:
            raise PublicationError("GitHub release flags are invalid")
        if not github["draft"] and not complete:
            raise PublicationError("published GitHub release has incomplete assets")
        if github["latest"] and github["draft"]:
            raise PublicationError("draft GitHub release cannot be latest")
        github_status = ("draft-complete" if complete else "draft-partial") if github["draft"] else ("public-latest" if github["latest"] else "public")

    expected_maven = identity["maven_entries"]
    maven = state["maven"]
    if not isinstance(maven, dict) or set(maven) != {"deployment_id", "deployment_name", "deployment_state", "bundle_sha256", "public_files"}:
        raise PublicationError("Maven state is invalid")
    deployment_id = maven["deployment_id"]
    deployment_state = maven["deployment_state"]
    if deployment_id is None:
        if any(maven[key] is not None for key in ("deployment_name", "deployment_state", "bundle_sha256")):
            raise PublicationError("Maven deployment state lacks an identifier")
    else:
        if not isinstance(deployment_id, str) or not deployment_id:
            raise PublicationError("Maven deployment identifier is invalid")
        if maven["deployment_name"] != identity["maven_bundle"]["deployment_name"]:
            raise PublicationError("Maven deployment name differs")
        if deployment_state not in MAVEN_STATES:
            raise PublicationError("Maven deployment state is unsupported")
        if maven["bundle_sha256"] != identity["maven_bundle"]["sha256"]:
            raise PublicationError("Maven deployment bundle bytes differ")
        if deployment_state == "FAILED":
            raise PublicationError("Maven deployment failed")
    maven_public = require_hash_map(maven["public_files"], expected_maven, "public Maven repository", partial=False)
    if maven_public:
        maven_status = "published"
    elif deployment_id is None:
        maven_status = "absent"
    elif deployment_state == "PUBLISHED":
        maven_status = "published-pending-public"
    else:
        maven_status = str(deployment_state).lower()

    npm = state["npm"]
    if not isinstance(npm, dict) or set(npm) != {"sha256", "dist_tags", "provenance"} or not isinstance(npm["dist_tags"], dict):
        raise PublicationError("npm state is invalid")
    npm_hash = npm["sha256"]
    if npm_hash is not None and npm_hash != identity["npm"]["sha256"]:
        raise PublicationError("npm package bytes differ")
    if not isinstance(npm["provenance"], bool):
        raise PublicationError("npm provenance state is invalid")
    for name, version in npm["dist_tags"].items():
        if not isinstance(name, str) or not isinstance(version, str):
            raise PublicationError("npm dist-tag state is invalid")
        if name in {"candidate", "latest"} and version == identity["version"] and npm_hash is None:
            raise PublicationError("npm dist-tag points to a missing package")
    if npm["dist_tags"].get("candidate") == identity["version"]:
        raise PublicationError("npm candidate dist-tag is obsolete")
    if npm_hash is None:
        if npm["provenance"]:
            raise PublicationError("npm provenance points to a missing package")
        npm_status = "absent"
    elif not npm["provenance"]:
        raise PublicationError("npm package provenance is missing")
    elif npm["dist_tags"].get("latest") != identity["version"]:
        raise PublicationError("npm package exists but is not published under latest")
    else:
        npm_status = "published-latest"

    complete = tag_status == "complete" and github_status == "public-latest" and maven_status == "published" and npm_status == "published-latest"
    if complete:
        next_operation = "complete"
    elif tag_status != "complete":
        next_operation = "create-tags"
    elif github_status in {"absent", "draft-partial", "draft-complete"}:
        next_operation = "publish-github"
    elif maven_status != "published":
        next_operation = "publish-maven"
    elif npm_status == "absent":
        next_operation = "publish-npm"
    else:
        next_operation = "promote-github"
    return {
        "source_tags": tag_status,
        "github": github_status,
        "maven": maven_status,
        "npm": npm_status,
        "next_operation": next_operation,
        "complete": complete,
    }


def request_bytes(url: str, token: str | None = None) -> bytes | None:
    headers = {"Accept": "application/vnd.github+json", "User-Agent": "synchro-release-verifier"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
        headers["X-GitHub-Api-Version"] = "2022-11-28"
    request = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            return response.read()
    except urllib.error.HTTPError as error:
        if error.code == 404:
            return None
        raise PublicationError(f"request failed with HTTP {error.code}: {url}") from error
    except urllib.error.URLError as error:
        raise PublicationError(f"request failed: {url}: {error.reason}") from error


def request_json(url: str, token: str | None = None) -> Any | None:
    data = request_bytes(url, token)
    if data is None:
        return None
    try:
        return json.loads(data)
    except json.JSONDecodeError as error:
        raise PublicationError(f"response is not JSON: {url}") from error


def central_authorization() -> str:
    username = os.environ.get("MAVEN_CENTRAL_USERNAME", "")
    password = os.environ.get("MAVEN_CENTRAL_PASSWORD", "")
    if not username or not password:
        raise PublicationError("MAVEN_CENTRAL_USERNAME and MAVEN_CENTRAL_PASSWORD are required")
    token = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("ascii")
    return f"Bearer {token}"


def central_request(path: str, *, data: bytes = b"", content_type: str | None = None, method: str = "POST") -> bytes:
    headers = {"Authorization": central_authorization(), "User-Agent": "synchro-release-publisher"}
    if content_type:
        headers["Content-Type"] = content_type
    request = urllib.request.Request(CENTRAL_API + path, data=data if method == "POST" else None, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=120) as response:
            return response.read()
    except urllib.error.HTTPError as error:
        raise PublicationError(f"Central request failed with HTTP {error.code}: {path}") from error
    except urllib.error.URLError as error:
        raise PublicationError(f"Central request failed: {path}: {error.reason}") from error


def central_json(path: str, *, method: str = "POST") -> Any:
    data = central_request(path, method=method)
    try:
        return json.loads(data)
    except json.JSONDecodeError as error:
        raise PublicationError(f"Central response is not JSON: {path}") from error


def central_upload(bundle: Path, name: str) -> str:
    if not bundle.is_file() or bundle.is_symlink():
        raise PublicationError("sealed Maven bundle is missing or unsafe")
    boundary = "synchro-release-boundary"
    body = (
        f"--{boundary}\r\n"
        f'Content-Disposition: form-data; name="bundle"; filename="{bundle.name}"\r\n'
        "Content-Type: application/octet-stream\r\n\r\n"
    ).encode("ascii") + bundle.read_bytes() + f"\r\n--{boundary}--\r\n".encode("ascii")
    query = urllib.parse.urlencode({"name": name, "publishingType": "USER_MANAGED"})
    identifier = central_request(f"/upload?{query}", data=body, content_type=f"multipart/form-data; boundary={boundary}").decode("ascii", "strict").strip()
    if not identifier or any(character.isspace() for character in identifier):
        raise PublicationError("Central upload returned an invalid deployment identifier")
    return identifier


def observe_public(identity: dict[str, Any], repository: str, token: str | None) -> dict[str, Any]:
    if repository != "trainstar/synchro":
        raise PublicationError("publication repository is invalid")
    api = f"https://api.github.com/repos/{repository}"
    tags: dict[str, str | None] = {}
    for tag in (identity["root_tag"], identity["go_tag"]):
        encoded = urllib.parse.quote(tag, safe="")
        value = request_json(f"{api}/git/ref/tags/{encoded}", token)
        tags[tag] = None if value is None else str(value.get("object", {}).get("sha", ""))

    release_value = request_json(f"{api}/releases/tags/{urllib.parse.quote(identity['root_tag'], safe='')}", token)
    latest_value = request_json(f"{api}/releases/latest", token)
    github: dict[str, Any] | None = None
    if release_value is not None:
        if not isinstance(release_value, dict) or not isinstance(release_value.get("assets"), list):
            raise PublicationError("GitHub release response is invalid")
        assets: dict[str, str] = {}
        for asset in release_value["assets"]:
            if not isinstance(asset, dict) or not isinstance(asset.get("name"), str) or not isinstance(asset.get("browser_download_url"), str):
                raise PublicationError("GitHub release asset response is invalid")
            digest_value = asset.get("digest")
            if release_value.get("draft") and digest_value is not None:
                digest = normalize_sha256(digest_value, "GitHub release asset digest")
            else:
                data = request_bytes(asset["browser_download_url"])
                if data is None:
                    raise PublicationError("GitHub release asset is unavailable")
                digest = sha256_bytes(data)
            if not SHA256.fullmatch(digest) or asset["name"] in assets:
                raise PublicationError("GitHub release asset is unavailable or duplicated")
            assets[asset["name"]] = digest
        latest = isinstance(latest_value, dict) and latest_value.get("id") == release_value.get("id")
        github = {"draft": release_value.get("draft"), "latest": latest, "assets": assets}

    version = identity["version"]
    npm_root = request_json(f"https://registry.npmjs.org/{urllib.parse.quote(NPM_PACKAGE, safe='@')}")
    npm_hash = None
    npm_provenance = False
    dist_tags: dict[str, str] = {}
    if npm_root is not None:
        if not isinstance(npm_root, dict):
            raise PublicationError("npm registry response is invalid")
        raw_tags = npm_root.get("dist-tags", {})
        if not isinstance(raw_tags, dict):
            raise PublicationError("npm registry dist-tags are invalid")
        dist_tags = {str(key): str(value) for key, value in raw_tags.items()}
        versions = npm_root.get("versions", {})
        if not isinstance(versions, dict):
            raise PublicationError("npm registry versions are invalid")
        package = versions.get(version)
        if package is not None:
            distribution = package.get("dist") if isinstance(package, dict) else None
            tarball_url = distribution.get("tarball") if isinstance(distribution, dict) else None
            if not isinstance(tarball_url, str):
                raise PublicationError("npm package tarball URL is missing")
            data = request_bytes(tarball_url)
            if data is None:
                raise PublicationError("npm package tarball is unavailable")
            npm_hash = sha256_bytes(data)
            npm_provenance = has_npm_provenance(distribution)

    public_maven: dict[str, str] = {}
    for relative in identity["maven_entries"]:
        data = request_bytes(f"{MAVEN_BASE}/{urllib.parse.quote(relative, safe='/.-')}")
        if data is not None:
            public_maven[relative] = sha256_bytes(data)
    return {
        "tags": tags,
        "github": github,
        "maven": {"deployment_id": None, "deployment_name": None, "deployment_state": None, "bundle_sha256": None, "public_files": public_maven},
        "npm": {"sha256": npm_hash, "dist_tags": dist_tags, "provenance": npm_provenance},
    }


def central_deployments(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict)]
    if isinstance(value, dict):
        for key in ("deployments", "items", "content"):
            if key in value:
                return central_deployments(value[key])
    raise PublicationError("Central deployment list response is invalid")


def select_central(value: Any, name: str) -> dict[str, Any] | None:
    matches = []
    for deployment in central_deployments(value):
        deployment_name = deployment.get("deploymentName", deployment.get("name"))
        if deployment_name == name:
            identifier = deployment.get("deploymentId", deployment.get("id"))
            state = deployment.get("deploymentState", deployment.get("state"))
            if not isinstance(identifier, str) or not identifier or state not in MAVEN_STATES:
                raise PublicationError("matching Central deployment is malformed")
            matches.append({"deployment_id": identifier, "deployment_name": name, "deployment_state": state})
    if len(matches) > 1:
        raise PublicationError("Central contains duplicate deterministic deployment names")
    return matches[0] if matches else None


def identity_for_directory(release_dir: Path) -> dict[str, Any]:
    identity = release_identity(release_dir)
    identity["maven_entries"] = maven_entries(release_dir / identity["maven_bundle"]["path"])
    return identity


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    identity_parser = subparsers.add_parser("identity")
    identity_parser.add_argument("--release-dir", type=Path, required=True)
    identity_parser.add_argument("--output", type=Path, required=True)
    classify_parser = subparsers.add_parser("classify")
    classify_parser.add_argument("--release-dir", type=Path, required=True)
    classify_parser.add_argument("--state", type=Path, required=True)
    classify_parser.add_argument("--output", type=Path, required=True)
    observe_parser = subparsers.add_parser("observe-public")
    observe_parser.add_argument("--release-dir", type=Path, required=True)
    observe_parser.add_argument("--repository", default="trainstar/synchro")
    observe_parser.add_argument("--github-token-environment", default="GITHUB_TOKEN")
    observe_parser.add_argument("--output", type=Path, required=True)
    select_parser = subparsers.add_parser("central-select")
    select_parser.add_argument("--input", type=Path, required=True)
    select_parser.add_argument("--name", required=True)
    select_parser.add_argument("--output", type=Path, required=True)
    central_list_parser = subparsers.add_parser("central-list")
    central_list_parser.add_argument("--name", required=True)
    central_list_parser.add_argument("--output", type=Path, required=True)
    central_status_parser = subparsers.add_parser("central-status")
    central_status_parser.add_argument("--deployment-id", required=True)
    central_status_parser.add_argument("--output", type=Path, required=True)
    central_upload_parser = subparsers.add_parser("central-upload")
    central_upload_parser.add_argument("--bundle", type=Path, required=True)
    central_upload_parser.add_argument("--name", required=True)
    central_upload_parser.add_argument("--output", type=Path, required=True)
    central_publish_parser = subparsers.add_parser("central-publish")
    central_publish_parser.add_argument("--deployment-id", required=True)
    central_drop_parser = subparsers.add_parser("central-drop")
    central_drop_parser.add_argument("--deployment-id", required=True)
    receipt_parser = subparsers.add_parser("verify-receipt")
    receipt_parser.add_argument("--receipt", type=Path, required=True)
    receipt_parser.add_argument("--artifact", type=Path, required=True)
    receipt_parser.add_argument("--release-manifest", type=Path, required=True)
    receipt_parser.add_argument("--expected-run-id", required=True)
    receipt_parser.add_argument("--output", type=Path, required=True)
    candidate_parser = subparsers.add_parser("validate-candidate")
    candidate_parser.add_argument("--source-commit", required=True)
    candidate_parser.add_argument("--version", required=True)
    args = parser.parse_args()
    try:
        if args.command == "identity":
            write_json(args.output, identity_for_directory(args.release_dir.resolve()))
        elif args.command == "classify":
            identity = identity_for_directory(args.release_dir.resolve())
            write_json(args.output, classify_publication(identity, load_json(args.state, "publication state")))
        elif args.command == "observe-public":
            identity = identity_for_directory(args.release_dir.resolve())
            token = os.environ.get(args.github_token_environment, "").strip() or None
            state = observe_public(identity, args.repository, token)
            write_json(args.output, state)
            write_json(args.output.with_name(args.output.stem + "-classification.json"), classify_publication(identity, state))
        elif args.command == "central-select":
            write_json(args.output, select_central(load_json(args.input, "Central deployments"), args.name))
        elif args.command == "central-list":
            query = urllib.parse.urlencode({"deploymentName": args.name, "pageSize": 100})
            write_json(args.output, central_json(f"/deployments?{query}", method="GET"))
        elif args.command == "central-status":
            query = urllib.parse.urlencode({"id": args.deployment_id})
            write_json(args.output, central_json(f"/status?{query}"))
        elif args.command == "central-upload":
            identifier = central_upload(args.bundle.resolve(), args.name)
            write_json(args.output, {"deployment_id": identifier})
        elif args.command == "central-publish":
            central_request(f"/deployment/{urllib.parse.quote(args.deployment_id, safe='')}")
        elif args.command == "central-drop":
            central_request(
                f"/deployment/{urllib.parse.quote(args.deployment_id, safe='')}",
                method="DELETE",
            )
        elif args.command == "verify-receipt":
            write_json(
                args.output,
                verify_sealed_receipt(
                    load_json(args.receipt, "sealed artifact receipt"),
                    load_json(args.artifact, "sealed artifact API response"),
                    load_json(args.release_manifest, "sealed release manifest"),
                    args.expected_run_id,
                ),
            )
        elif args.command == "validate-candidate":
            validate_candidate_identity(args.source_commit, args.version)
        else:
            raise PublicationError(f"unsupported command {args.command}")
    except PublicationError as error:
        print(f"release-publish: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
