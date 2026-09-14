#!/usr/bin/env python3
"""Generate fail-closed packaged smoke cell and summary evidence."""

from __future__ import annotations

import argparse
import base64
import hashlib
import hmac
import json
import os
import re
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit


SMOKE_OPERATIONS = ("connect", "push", "pull", "kill", "resume")
CELL_SCHEMA_VERSION = 1
SUMMARY_SCHEMA_VERSION = 1
SHA256 = re.compile(r"^[0-9a-f]{64}$")
COMMIT = re.compile(r"^[0-9a-f]{40}$")
PUBLIC_CONSUMER_FORBIDDEN = (
    "@_spi(Inspection)",
    "SynchroInspection",
    "TransportObservationCollector",
    "TransportOperationClass",
    "withTransportObservation",
    "com.trainstar.synchro.inspection",
    "@trainstar/synchro-react-native/inspection",
    "mavenLocal()",
    ".package(path:",
    "npm install file:",
    "pod 'Synchro', :path =>",
)


class EvidenceError(ValueError):
    """Describe one packaged smoke evidence error."""


def load_json(path: Path, label: str) -> Any:
    try:
        with path.open(encoding="utf-8") as stream:
            return json.load(stream)
    except (OSError, json.JSONDecodeError) as error:
        raise EvidenceError(f"{label} is missing or malformed: {error}") from error


def write_json(path: Path, value: object, mode: int = 0o644) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, mode)
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            json.dump(value, stream, indent=2, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise


def source_commit(repo_root: Path) -> str:
    try:
        commit = subprocess.check_output(
            ["git", "-C", str(repo_root), "rev-parse", "--verify", "HEAD"],
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except (OSError, subprocess.CalledProcessError) as error:
        raise EvidenceError(f"cannot resolve the repository commit: {error}") from error
    if not COMMIT.fullmatch(commit):
        raise EvidenceError("repository HEAD is not a full commit hash")
    return commit


def required_cells(repo_root: Path) -> list[str]:
    matrix = load_json(repo_root / "conformance/support-matrix.json", "support matrix")
    if not isinstance(matrix, dict) or not isinstance(matrix.get("cells"), list):
        raise EvidenceError("support matrix does not declare cells")
    result: list[str] = []
    seen: set[str] = set()
    for raw_cell in matrix["cells"]:
        if not isinstance(raw_cell, dict):
            raise EvidenceError("support matrix contains a malformed cell")
        cell_id = raw_cell.get("id")
        if not isinstance(cell_id, str) or not cell_id:
            raise EvidenceError("support matrix contains a cell without an id")
        if cell_id in seen:
            raise EvidenceError(f"support matrix repeats cell {cell_id}")
        seen.add(cell_id)
        if raw_cell.get("policy") == "required":
            result.append(cell_id)
    if not result:
        raise EvidenceError("support matrix has no packaged smoke cells")
    return result


def hash_files(paths: list[Path]) -> list[str]:
    hashes: list[str] = []
    seen: set[str] = set()
    for path in paths:
        try:
            data = path.read_bytes()
        except OSError as error:
            raise EvidenceError(f"cannot read packaged artifact {path}: {error}") from error
        digest = hashlib.sha256(data).hexdigest()
        if digest not in seen:
            hashes.append(digest)
            seen.add(digest)
    return hashes


def validate_public_consumer_sources(consumer_root: Path) -> None:
    if not consumer_root.is_dir():
        raise EvidenceError(f"public consumer root is missing: {consumer_root}")
    source_files = [
        path
        for path in consumer_root.rglob("*")
        if path.is_file()
        and path.name != "Package.swift"
        and path.suffix in {".go", ".kt", ".swift", ".ts", ".tsx", ".sh", ".py"}
    ]
    if not source_files:
        raise EvidenceError(f"public consumer root has no source files: {consumer_root}")
    for path in source_files:
        try:
            content = path.read_text(encoding="utf-8")
        except OSError as error:
            raise EvidenceError(f"cannot read public consumer source {path}: {error}") from error
        for forbidden in PUBLIC_CONSUMER_FORBIDDEN:
            if forbidden in content:
                raise EvidenceError(f"public consumer source imports or resolves forbidden dependency {forbidden}: {path}")


def operation_entries(status: str, test_count: int) -> list[dict[str, object]]:
    return [
        {
            "name": operation,
            "status": status,
            "terminal": True,
            "test_count": test_count,
        }
        for operation in SMOKE_OPERATIONS
    ]


def begin_cell(repo_root: Path, cell_id: str, output: Path) -> None:
    if cell_id not in required_cells(repo_root):
        raise EvidenceError(f"unknown packaged smoke cell {cell_id}")
    write_json(
        output,
        {
            "schema_version": CELL_SCHEMA_VERSION,
            "cell_id": cell_id,
            "source_commit": source_commit(repo_root),
            "status": "failed",
            "artifact_hashes": [],
            "operations": operation_entries("failed", 0),
            "failure": "packaged smoke cell did not reach a terminal pass",
        },
    )


def required_integer(value: object, field: str, minimum: int = 0) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < minimum:
        raise EvidenceError(f"{field} must be an integer greater than or equal to {minimum}")
    return value


def validate_phase(path: Path, expected_phase: str) -> dict[str, object]:
    value = load_json(path, f"{expected_phase} phase result")
    if not isinstance(value, dict):
        raise EvidenceError(f"{expected_phase} phase result must be an object")
    expected_keys = {"schema_version", "phase", "status", "pid", "pending_change_count"}
    if set(value) != expected_keys:
        raise EvidenceError(f"{expected_phase} phase result has invalid members")
    if value.get("schema_version") != 1 or value.get("phase") != expected_phase:
        raise EvidenceError(f"{expected_phase} phase result has invalid identity")
    if value.get("status") != "passed":
        raise EvidenceError(f"{expected_phase} phase did not pass")
    required_integer(value.get("pid"), f"{expected_phase} pid", 1)
    required_integer(value.get("pending_change_count"), f"{expected_phase} pending count")
    return value


def complete_cell(
    repo_root: Path,
    cell_id: str,
    output: Path,
    initial_path: Path,
    resume_path: Path,
    killed_pid: int,
    artifacts: list[Path],
    expected_hashes: list[str],
) -> None:
    if cell_id not in required_cells(repo_root):
        raise EvidenceError(f"unknown packaged smoke cell {cell_id}")
    initial = validate_phase(initial_path, "initial")
    resume = validate_phase(resume_path, "resume")
    initial_pid = required_integer(initial["pid"], "initial pid", 1)
    resume_pid = required_integer(resume["pid"], "resume pid", 1)
    if killed_pid != initial_pid:
        raise EvidenceError("killed pid does not match the initial consumer process")
    if resume_pid == initial_pid:
        raise EvidenceError("resume reused the killed consumer process")
    pending_before = required_integer(initial["pending_change_count"], "initial pending count")
    pending_after = required_integer(resume["pending_change_count"], "resume pending count")
    if pending_before <= 0:
        raise EvidenceError("initial consumer did not persist pending work before process kill")
    if pending_after != 0:
        raise EvidenceError("resumed consumer did not drain its durable pending work")
    hashes = hash_files(artifacts)
    if not hashes:
        raise EvidenceError("packaged smoke cell has no packaged artifact hash")
    expected = validate_hash_list(expected_hashes, "expected artifact hashes")
    if sorted(hashes) != sorted(expected):
        raise EvidenceError("packaged smoke artifact hashes do not match sealed artifact hashes")
    write_json(
        output,
        {
            "schema_version": CELL_SCHEMA_VERSION,
            "cell_id": cell_id,
            "source_commit": source_commit(repo_root),
            "status": "passed",
            "artifact_hashes": hashes,
            "operations": operation_entries("passed", 1),
            "process_lifecycle": {
                "initial_pid": initial_pid,
                "kill_signal": 9,
                "resume_pid": resume_pid,
                "durable_pending_before_kill": pending_before,
                "durable_pending_after_resume": pending_after,
            },
        },
    )


def validate_server_phase(path: Path, expected_phase: str) -> dict[str, object]:
    value = load_json(path, f"server {expected_phase} phase result")
    expected = {"schema_version", "phase", "status", "adapter_pid", "push_digest"}
    if expected_phase == "resume":
        expected.add("replay_equal")
    if not isinstance(value, dict) or set(value) != expected:
        raise EvidenceError(f"server {expected_phase} phase result has invalid members")
    if value.get("schema_version") != 1 or value.get("phase") != expected_phase or value.get("status") != "passed":
        raise EvidenceError(f"server {expected_phase} phase result is invalid")
    required_integer(value.get("adapter_pid"), f"server {expected_phase} adapter pid", 1)
    if not isinstance(value.get("push_digest"), str) or not SHA256.fullmatch(value["push_digest"]):
        raise EvidenceError(f"server {expected_phase} push digest is invalid")
    if expected_phase == "resume" and value.get("replay_equal") is not True:
        raise EvidenceError("server replay response differs from the persisted response")
    return value


def complete_server_cell(repo_root: Path, cell_id: str, output: Path, initial_path: Path, resume_path: Path, killed_pid: int, artifacts: list[Path], expected_hashes: list[str]) -> None:
    if cell_id not in required_cells(repo_root):
        raise EvidenceError(f"unknown packaged smoke cell {cell_id}")
    initial = validate_server_phase(initial_path, "initial")
    resume = validate_server_phase(resume_path, "resume")
    initial_pid = required_integer(initial["adapter_pid"], "server initial adapter pid", 1)
    resume_pid = required_integer(resume["adapter_pid"], "server resume adapter pid", 1)
    if killed_pid != initial_pid or resume_pid == initial_pid:
        raise EvidenceError("server adapter process replacement proof is invalid")
    if initial["push_digest"] != resume["push_digest"]:
        raise EvidenceError("server replay digest differs from the persisted push")
    hashes = hash_files(artifacts)
    if sorted(hashes) != sorted(validate_hash_list(expected_hashes, "expected artifact hashes")):
        raise EvidenceError("packaged smoke artifact hashes do not match sealed artifact hashes")
    write_json(
        output,
        {
            "schema_version": CELL_SCHEMA_VERSION,
            "cell_id": cell_id,
            "source_commit": source_commit(repo_root),
            "status": "passed",
            "artifact_hashes": hashes,
            "operations": operation_entries("passed", 1),
            "process_lifecycle": {
                "kind": "server",
                "initial_pid": initial_pid,
                "kill_signal": 9,
                "resume_pid": resume_pid,
                "push_digest": initial["push_digest"],
                "replay_equal": True,
            },
        },
    )


def validate_hash_list(value: object, field: str, allow_empty: bool = False) -> list[str]:
    if not isinstance(value, list) or (not value and not allow_empty):
        raise EvidenceError(f"{field} must contain artifact hashes")
    result: list[str] = []
    for item in value:
        if not isinstance(item, str) or not SHA256.fullmatch(item):
            raise EvidenceError(f"{field} contains an invalid artifact hash")
        result.append(item)
    if len(result) != len(set(result)):
        raise EvidenceError(f"{field} repeats an artifact hash")
    return result


def validate_cell(value: object, expected_cell: str, expected_commit: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise EvidenceError(f"cell {expected_cell} evidence must be an object")
    if value.get("schema_version") != CELL_SCHEMA_VERSION:
        raise EvidenceError(f"cell {expected_cell} has the wrong schema version")
    if value.get("cell_id") != expected_cell:
        raise EvidenceError(f"cell {expected_cell} has the wrong identity")
    if value.get("source_commit") != expected_commit:
        raise EvidenceError(f"cell {expected_cell} source commit does not match HEAD")
    status = value.get("status")
    if status not in {"passed", "failed"}:
        raise EvidenceError(f"cell {expected_cell} has an invalid status")
    expected_members = {
        "schema_version",
        "cell_id",
        "source_commit",
        "status",
        "artifact_hashes",
        "operations",
        "process_lifecycle" if status == "passed" else "failure",
    }
    if set(value) != expected_members:
        raise EvidenceError(f"cell {expected_cell} has invalid members")
    failure = value.get("failure")
    if status == "failed" and (not isinstance(failure, str) or not failure):
        raise EvidenceError(f"cell {expected_cell} failure is missing")
    hashes = validate_hash_list(
        value.get("artifact_hashes"),
        f"cell {expected_cell} artifact_hashes",
        allow_empty=status == "failed",
    )
    operations = value.get("operations")
    if not isinstance(operations, list):
        raise EvidenceError(f"cell {expected_cell} operations are missing")
    actual_names: list[str] = []
    for item in operations:
        if not isinstance(item, dict):
            raise EvidenceError(f"cell {expected_cell} contains a malformed operation")
        if set(item) != {"name", "status", "terminal", "test_count"}:
            raise EvidenceError(f"cell {expected_cell} contains invalid operation members")
        name = item.get("name")
        if not isinstance(name, str):
            raise EvidenceError(f"cell {expected_cell} contains an unnamed operation")
        actual_names.append(name)
        if item.get("status") != status:
            raise EvidenceError(f"cell {expected_cell} operation {name} has the wrong status")
        if item.get("terminal") is not True:
            raise EvidenceError(f"cell {expected_cell} operation {name} is not terminal")
        expected_count = 1 if status == "passed" else 0
        if item.get("test_count") != expected_count:
            raise EvidenceError(f"cell {expected_cell} operation {name} has an invalid test count")
    if actual_names != list(SMOKE_OPERATIONS):
        raise EvidenceError(f"cell {expected_cell} does not contain each smoke operation once")
    if status == "passed":
        lifecycle = value.get("process_lifecycle")
        if not isinstance(lifecycle, dict):
            raise EvidenceError(f"cell {expected_cell} process lifecycle is missing")
        kind = lifecycle.get("kind", "client")
        common_keys = {"initial_pid", "kill_signal", "resume_pid"}
        client_keys = common_keys | {"durable_pending_before_kill", "durable_pending_after_resume"}
        server_keys = common_keys | {"kind", "push_digest", "replay_equal"}
        lifecycle_keys = set(lifecycle)
        if lifecycle_keys != client_keys and lifecycle_keys != server_keys:
            raise EvidenceError(f"cell {expected_cell} process lifecycle has invalid members")
        initial_pid = required_integer(lifecycle.get("initial_pid"), "initial pid", 1)
        resume_pid = required_integer(lifecycle.get("resume_pid"), "resume pid", 1)
        if lifecycle.get("kill_signal") != 9 or initial_pid == resume_pid:
            raise EvidenceError(f"cell {expected_cell} process kill proof is invalid")
        if kind == "server":
            if (
                not isinstance(lifecycle.get("push_digest"), str)
                or not SHA256.fullmatch(lifecycle["push_digest"])
                or lifecycle.get("replay_equal") is not True
            ):
                raise EvidenceError(f"cell {expected_cell} server replay proof is invalid")
        else:
            if required_integer(lifecycle.get("durable_pending_before_kill"), "pending before kill") <= 0:
                raise EvidenceError(f"cell {expected_cell} has no durable work before kill")
            if lifecycle.get("durable_pending_after_resume") != 0:
                raise EvidenceError(f"cell {expected_cell} did not drain durable work after resume")
    return {
        "status": status,
        "artifact_hashes": hashes,
        "operations": operations,
    }


def missing_cell(cell_id: str) -> dict[str, object]:
    return {
        "status": "failed",
        "artifact_hashes": [],
        "operations": operation_entries("failed", 0),
        "failure": f"cell evidence is missing for {cell_id}",
    }


def collect_summary(repo_root: Path, cells_dir: Path, output: Path) -> None:
    commit = source_commit(repo_root)
    cells = required_cells(repo_root)
    if cells_dir.exists():
        evidence_cells = {path.stem for path in cells_dir.glob("*.json")}
        unexpected = evidence_cells.difference(cells)
        if unexpected:
            raise EvidenceError(f"packaged smoke has unexpected cell evidence: {', '.join(sorted(unexpected))}")
    records: dict[str, dict[str, object]] = {}
    for cell_id in cells:
        path = cells_dir / f"{cell_id}.json"
        if not path.is_file():
            records[cell_id] = missing_cell(cell_id)
            continue
        records[cell_id] = validate_cell(load_json(path, f"cell {cell_id}"), cell_id, commit)

    hashes = sorted(
        {
            digest
            for record in records.values()
            for digest in record.get("artifact_hashes", [])
            if isinstance(digest, str)
        }
    )
    obligations: list[dict[str, object]] = []
    all_passed = True
    for cell_id in cells:
        record = records[cell_id]
        record_status = record["status"]
        if record_status != "passed":
            all_passed = False
        record_hashes = record.get("artifact_hashes", [])
        for operation in record["operations"]:
            if not isinstance(operation, dict):
                raise EvidenceError(f"cell {cell_id} contains a malformed operation")
            obligations.append(
                {
                    "id": f"smoke/{cell_id}/{operation['name']}",
                    "kind": "smoke",
                    "status": operation["status"],
                    "terminal": operation["terminal"],
                    "test_count": operation["test_count"],
                    "artifact_hashes": record_hashes,
                }
            )
    write_json(
        output,
        {
            "schema_version": SUMMARY_SCHEMA_VERSION,
            "source_commit": commit,
            "artifact_hashes": hashes,
            "obligations": obligations,
            "status": "passed" if all_passed else "failed",
        },
    )


def dry_summary(repo_root: Path, output: Path) -> None:
    commit = source_commit(repo_root)
    structural_hash = hashlib.sha256((repo_root / "verification/packaged-smoke-cell.schema.json").read_bytes()).hexdigest()
    obligations = [
        {
            "id": f"smoke/{cell_id}/{operation}",
            "kind": "smoke",
            "status": "passed",
            "terminal": True,
            "test_count": 1,
            "artifact_hashes": [structural_hash],
        }
        for cell_id in required_cells(repo_root)
        for operation in SMOKE_OPERATIONS
    ]
    write_json(
        output,
        {
            "schema_version": SUMMARY_SCHEMA_VERSION,
            "source_commit": commit,
            "artifact_hashes": [structural_hash],
            "obligations": obligations,
            "status": "passed",
            "dry_run": True,
        },
    )


def base64url(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def bearer_token(subject: str) -> str:
    supplied = os.environ.get("SYNCHRO_PACKAGED_SMOKE_TOKEN", "").strip()
    if supplied:
        return supplied
    secret = os.environ.get("SYNCHRO_TEST_JWT_SECRET", "")
    if not secret:
        secret_file = os.environ.get("SYNCHRO_CONFORMANCE_JWT_SECRET_FILE", "").strip()
        if secret_file:
            try:
                secret = Path(secret_file).read_text(encoding="utf-8").strip()
            except OSError as error:
                raise EvidenceError(f"cannot read the packaged smoke JWT secret file: {error}") from error
    if not secret:
        raise EvidenceError(
            "SYNCHRO_PACKAGED_SMOKE_TOKEN, SYNCHRO_TEST_JWT_SECRET, or "
            "SYNCHRO_CONFORMANCE_JWT_SECRET_FILE is required"
        )
    now = int(time.time())
    header = base64url(b'{"alg":"HS256","typ":"JWT"}')
    payload = base64url(
        json.dumps(
            {"sub": subject, "iat": now, "exp": now + 3600},
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    )
    signing_input = f"{header}.{payload}"
    signature = hmac.new(secret.encode("utf-8"), signing_input.encode("ascii"), hashlib.sha256).digest()
    return f"{signing_input}.{base64url(signature)}"


def smoke_config(cell_id: str, platform: str) -> dict[str, str | int]:
    server_url = os.environ.get("SYNCHRO_TEST_URL", "").strip().rstrip("/")
    parsed = urlsplit(server_url)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname or parsed.username or parsed.password:
        raise EvidenceError("SYNCHRO_TEST_URL must be an HTTP URL without embedded credentials")
    if parsed.query or parsed.fragment:
        raise EvidenceError("SYNCHRO_TEST_URL must not contain a query or fragment")
    user_id = str(uuid.uuid4())
    return {
        "schema_version": 1,
        "cell_id": cell_id,
        "platform": platform,
        "server_url": server_url,
        "token": bearer_token(user_id),
        "user_id": user_id,
        "client_id": str(uuid.uuid4()),
        "customer_id": str(uuid.uuid4()),
        "order_id": str(uuid.uuid4()),
        "phase": "initial",
    }


def write_config(cell_id: str, platform: str, output: Path) -> None:
    write_json(output, smoke_config(cell_id, platform), mode=0o600)


def write_typescript(value: dict[str, object], output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    serialized = json.dumps(value, separators=(",", ":"), sort_keys=True)
    content = "export const packagedSmokeConfig = " + serialized + " as const;\n"
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{output.name}.", dir=output.parent)
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, output)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise


def config_to_typescript(config_path: Path, output: Path) -> None:
    value = load_json(config_path, "packaged smoke config")
    if not isinstance(value, dict) or value.get("schema_version") != 1:
        raise EvidenceError("packaged smoke config is invalid")
    write_typescript(value, output)


def config_value(path: Path, field: str) -> None:
    value = load_json(path, "packaged smoke config")
    if not isinstance(value, dict) or field not in value:
        raise EvidenceError(f"packaged smoke config field {field} is missing")
    item = value[field]
    if not isinstance(item, (str, int)) or isinstance(item, bool):
        raise EvidenceError(f"packaged smoke config field {field} is invalid")
    print(item)


def set_config_phase(path: Path, phase: str, output: Path) -> None:
    value = load_json(path, "packaged smoke config")
    if not isinstance(value, dict) or value.get("schema_version") != 1:
        raise EvidenceError("packaged smoke config is invalid")
    if phase not in {"initial", "resume"}:
        raise EvidenceError("packaged smoke phase is invalid")
    value["phase"] = phase
    write_json(output, value, mode=0o600)


def write_phase(path: Path, phase: str, pid: int, pending_count: int) -> None:
    write_json(
        path,
        {
            "schema_version": 1,
            "phase": phase,
            "status": "passed",
            "pid": pid,
            "pending_change_count": pending_count,
        },
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    begin = subparsers.add_parser("begin-cell")
    begin.add_argument("--repo-root", type=Path, required=True)
    begin.add_argument("--cell", required=True)
    begin.add_argument("--output", type=Path, required=True)

    complete = subparsers.add_parser("complete-cell")
    complete.add_argument("--repo-root", type=Path, required=True)
    complete.add_argument("--cell", required=True)
    complete.add_argument("--output", type=Path, required=True)
    complete.add_argument("--initial", type=Path, required=True)
    complete.add_argument("--resume", type=Path, required=True)
    complete.add_argument("--killed-pid", type=int, required=True)
    complete.add_argument("--artifact", action="append", type=Path, default=[])
    complete.add_argument("--expected-artifact-hash", action="append", default=[])

    server_complete = subparsers.add_parser("complete-server-cell")
    for argument in ("--repo-root", "--cell", "--output", "--initial", "--resume"):
        server_complete.add_argument(argument, type=Path if argument in {"--repo-root", "--output", "--initial", "--resume"} else str, required=True)
    server_complete.add_argument("--killed-pid", type=int, required=True)
    server_complete.add_argument("--artifact", action="append", type=Path, default=[])
    server_complete.add_argument("--expected-artifact-hash", action="append", default=[])

    collect = subparsers.add_parser("collect")
    collect.add_argument("--repo-root", type=Path, required=True)
    collect.add_argument("--cells-dir", type=Path, required=True)
    collect.add_argument("--output", type=Path, required=True)

    dry = subparsers.add_parser("dry-run")
    dry.add_argument("--repo-root", type=Path, required=True)
    dry.add_argument("--output", type=Path, required=True)

    config = subparsers.add_parser("config")
    config.add_argument("--cell", required=True)
    config.add_argument("--platform", required=True)
    config.add_argument("--output", type=Path, required=True)

    config_ts = subparsers.add_parser("config-to-typescript")
    config_ts.add_argument("--config", type=Path, required=True)
    config_ts.add_argument("--output", type=Path, required=True)

    get = subparsers.add_parser("config-value")
    get.add_argument("--config", type=Path, required=True)
    get.add_argument("--field", required=True)

    set_phase = subparsers.add_parser("set-config-phase")
    set_phase.add_argument("--config", type=Path, required=True)
    set_phase.add_argument("--phase", choices=("initial", "resume"), required=True)
    set_phase.add_argument("--output", type=Path, required=True)

    phase = subparsers.add_parser("phase-result")
    phase.add_argument("--output", type=Path, required=True)
    phase.add_argument("--phase", choices=("initial", "resume"), required=True)
    phase.add_argument("--pid", type=int, required=True)
    phase.add_argument("--pending-count", type=int, required=True)

    public_imports = subparsers.add_parser("public-import-check")
    public_imports.add_argument("--consumer-root", type=Path, action="append", required=True)

    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        if args.command == "begin-cell":
            begin_cell(args.repo_root.resolve(), args.cell, args.output.resolve())
        elif args.command == "complete-cell":
            complete_cell(
                args.repo_root.resolve(),
                args.cell,
                args.output.resolve(),
                args.initial.resolve(),
                args.resume.resolve(),
                args.killed_pid,
                [path.resolve() for path in args.artifact],
                args.expected_artifact_hash,
            )
        elif args.command == "complete-server-cell":
            complete_server_cell(args.repo_root.resolve(), args.cell, args.output.resolve(), args.initial.resolve(), args.resume.resolve(), args.killed_pid, [path.resolve() for path in args.artifact], args.expected_artifact_hash)
        elif args.command == "collect":
            collect_summary(args.repo_root.resolve(), args.cells_dir.resolve(), args.output.resolve())
        elif args.command == "dry-run":
            dry_summary(args.repo_root.resolve(), args.output.resolve())
        elif args.command == "config":
            write_config(args.cell, args.platform, args.output.resolve())
        elif args.command == "config-to-typescript":
            config_to_typescript(args.config.resolve(), args.output.resolve())
        elif args.command == "config-value":
            config_value(args.config.resolve(), args.field)
        elif args.command == "set-config-phase":
            set_config_phase(args.config.resolve(), args.phase, args.output.resolve())
        elif args.command == "phase-result":
            write_phase(args.output.resolve(), args.phase, args.pid, args.pending_count)
        elif args.command == "public-import-check":
            for consumer_root in args.consumer_root:
                validate_public_consumer_sources(consumer_root.resolve())
        else:
            raise EvidenceError(f"unsupported command {args.command}")
    except EvidenceError as error:
        print(f"packaged-smoke: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
