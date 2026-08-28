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
        if raw_cell.get("policy") != "excluded":
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
        expected_lifecycle_keys = {
            "initial_pid",
            "kill_signal",
            "resume_pid",
            "durable_pending_before_kill",
            "durable_pending_after_resume",
        }
        if set(lifecycle) != expected_lifecycle_keys:
            raise EvidenceError(f"cell {expected_cell} process lifecycle has invalid members")
        initial_pid = required_integer(lifecycle.get("initial_pid"), "initial pid", 1)
        resume_pid = required_integer(lifecycle.get("resume_pid"), "resume pid", 1)
        if lifecycle.get("kill_signal") != 9 or initial_pid == resume_pid:
            raise EvidenceError(f"cell {expected_cell} process kill proof is invalid")
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
            )
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
        else:
            raise EvidenceError(f"unsupported command {args.command}")
    except EvidenceError as error:
        print(f"packaged-smoke: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
