#!/usr/bin/env python3
"""Seal and verify release-candidate artifacts and write the RC manifest."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import re
import subprocess
import tarfile
import tempfile
from pathlib import Path
from typing import Any


CANDIDATE = re.compile(r"^RC-0\.3\.0-[0-9]{8}T[0-9]{6}Z-[0-9a-f]{7,64}$")
COMMIT = re.compile(r"^[0-9a-f]{40}$")
SHA256 = re.compile(r"^[0-9a-f]{64}$")
CHECKSUMS_NAME = "artifact-checksums.txt"
MANIFEST_NAME = "rc-manifest.json"
METADATA_NAME = "candidate-metadata.json"
REQUIRED_ARTIFACTS = (
    "artifacts/adapter/synchrod-pg",
    "artifacts/adapter/synchrod-pg.sha256",
    "artifacts/extension/artifact-manifest.json",
    "artifacts/extension/artifact-manifest.json.sha256",
    "artifacts/synchro-pg-pg18-linux-x64.tar.gz",
    "artifacts/clients/apple/Synchro/Package.swift",
    "artifacts/clients/apple/Synchro/Synchro.podspec",
    "artifacts/clients/apple/synchro-spm-0.3.0.tar.gz",
    "artifacts/clients/maven/fit/trainstar/synchro/0.3.0/synchro-0.3.0.aar",
    "artifacts/clients/npm/trainstar-synchro-react-native-0.3.0.tgz",
)


class RCError(ValueError):
    """Describe one fail-closed release-candidate error."""


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError as error:
        raise RCError(f"cannot hash artifact {path}: {error}") from error
    return digest.hexdigest()


def validate_candidate(candidate_dir: Path, candidate_id: str) -> None:
    if not CANDIDATE.fullmatch(candidate_id):
        raise RCError("candidate ID must match RC-0.3.0-YYYYMMDDTHHMMSSZ-<commit>")
    if candidate_dir.name != candidate_id:
        raise RCError("candidate directory name does not match the candidate ID")


def validate_extension_archive(candidate_dir: Path) -> None:
    bundle = candidate_dir / "artifacts/extension"
    archive_path = candidate_dir / "artifacts/synchro-pg-pg18-linux-x64.tar.gz"
    expected = {
        path.relative_to(bundle).as_posix(): file_sha256(path)
        for path in bundle.rglob("*")
        if path.is_file()
    }
    actual: dict[str, str] = {}
    try:
        with tarfile.open(archive_path, "r:gz") as archive:
            for member in archive.getmembers():
                path = Path(member.name)
                if path.is_absolute() or ".." in path.parts or path.parts[:1] != ("extension",):
                    raise RCError("extension archive contains an unsafe path")
                if member.issym() or member.islnk():
                    raise RCError("extension archive contains a link")
                if member.isfile():
                    relative = Path(*path.parts[1:]).as_posix()
                    stream = archive.extractfile(member)
                    if stream is None or relative in actual:
                        raise RCError("extension archive contains an invalid file")
                    actual[relative] = hashlib.sha256(stream.read()).hexdigest()
                elif not member.isdir():
                    raise RCError("extension archive contains an unsupported entry")
    except (OSError, tarfile.TarError) as error:
        raise RCError(f"extension archive is invalid: {error}") from error
    if actual != expected:
        raise RCError("extension archive does not match the staged extension bundle")


def artifact_files(candidate_dir: Path) -> list[Path]:
    root = candidate_dir / "artifacts"
    if not root.is_dir() or root.is_symlink():
        raise RCError("candidate artifact directory is missing or unsafe")
    files: list[Path] = []
    for path in root.rglob("*"):
        if path.is_symlink():
            raise RCError(f"candidate artifacts contain a symbolic link: {path.relative_to(candidate_dir)}")
        if path.is_file():
            files.append(path)
        elif not path.is_dir():
            raise RCError(f"candidate artifacts contain an unsupported entry: {path.relative_to(candidate_dir)}")
    files.sort(key=lambda path: path.relative_to(candidate_dir).as_posix())
    if not files:
        raise RCError("candidate contains no artifacts")
    for relative in REQUIRED_ARTIFACTS:
        if not (candidate_dir / relative).is_file():
            raise RCError(f"required candidate artifact is missing: {relative}")
    return files


def checksum_records(candidate_dir: Path) -> list[dict[str, Any]]:
    validate_extension_archive(candidate_dir)
    return [
        {
            "path": path.relative_to(candidate_dir).as_posix(),
            "size_bytes": path.stat().st_size,
            "sha256": file_sha256(path),
        }
        for path in artifact_files(candidate_dir)
    ]


def write_atomic(path: Path, data: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise


def seal(candidate_dir: Path, candidate_id: str, source_commit: str) -> None:
    validate_candidate(candidate_dir, candidate_id)
    if not COMMIT.fullmatch(source_commit):
        raise RCError("source commit must be a full Git commit hash")
    checksum_path = candidate_dir / CHECKSUMS_NAME
    metadata_path = candidate_dir / METADATA_NAME
    if checksum_path.exists() or metadata_path.exists():
        raise RCError("candidate artifacts are already sealed")
    records = checksum_records(candidate_dir)
    lines = [f"{record['sha256']}  {record['path']}" for record in records]
    write_atomic(checksum_path, "\n".join(lines) + "\n")
    write_atomic(
        metadata_path,
        json.dumps(
            {
                "schema_version": 1,
                "candidate_id": candidate_id,
                "release_version": "0.3.0",
                "source_commit": source_commit,
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
    )


def load_checksums(candidate_dir: Path) -> list[dict[str, Any]]:
    checksum_path = candidate_dir / CHECKSUMS_NAME
    try:
        lines = checksum_path.read_text(encoding="utf-8").splitlines()
    except OSError as error:
        raise RCError(f"candidate checksum file is missing: {error}") from error
    if not lines:
        raise RCError("candidate checksum file is empty")
    records: list[dict[str, Any]] = []
    seen: set[str] = set()
    for line in lines:
        digest, separator, relative = line.partition("  ")
        if not separator or not SHA256.fullmatch(digest) or not relative.startswith("artifacts/"):
            raise RCError("candidate checksum file is malformed")
        path = Path(relative)
        if path.is_absolute() or ".." in path.parts or relative in seen:
            raise RCError("candidate checksum file contains an unsafe or duplicate path")
        seen.add(relative)
        artifact = candidate_dir / path
        if not artifact.is_file() or artifact.is_symlink():
            raise RCError(f"candidate artifact is missing or unsafe: {relative}")
        actual = file_sha256(artifact)
        if actual != digest:
            raise RCError(f"candidate artifact hash mismatch: {relative}")
        records.append({"path": relative, "size_bytes": artifact.stat().st_size, "sha256": digest})
    current = [path.relative_to(candidate_dir).as_posix() for path in artifact_files(candidate_dir)]
    if current != [record["path"] for record in records]:
        raise RCError("candidate artifact set changed after hashing")
    return records


def verify(candidate_dir: Path, candidate_id: str, source_commit: str | None = None) -> list[dict[str, Any]]:
    validate_candidate(candidate_dir, candidate_id)
    metadata = load_json(candidate_dir / METADATA_NAME, "candidate metadata")
    expected = {"schema_version", "candidate_id", "release_version", "source_commit"}
    if set(metadata) != expected or metadata.get("schema_version") != 1 or metadata.get("candidate_id") != candidate_id or metadata.get("release_version") != "0.3.0" or not COMMIT.fullmatch(str(metadata.get("source_commit", ""))):
        raise RCError("candidate metadata is invalid")
    if source_commit is not None and metadata["source_commit"] != source_commit:
        raise RCError("candidate source commit does not match the requested commit")
    records = load_checksums(candidate_dir)
    validate_extension_archive(candidate_dir)
    return records


def run_verified(candidate_dir: Path, candidate_id: str, source_commit: str, command: list[str]) -> int:
    if not command:
        raise RCError("verified execution requires a command")
    checksum_path = candidate_dir / CHECKSUMS_NAME
    metadata_path = candidate_dir / METADATA_NAME
    expected_control_hashes = (file_sha256(checksum_path), file_sha256(metadata_path))
    expected_records = verify(candidate_dir, candidate_id, source_commit)
    status = subprocess.run(command, check=False).returncode
    actual_records = verify(candidate_dir, candidate_id, source_commit)
    actual_control_hashes = (file_sha256(checksum_path), file_sha256(metadata_path))
    if actual_records != expected_records or actual_control_hashes != expected_control_hashes:
        raise RCError("candidate artifact identity changed during execution")
    return status


def archive_extension(source: Path, output: Path) -> None:
    if output.exists():
        raise RCError("extension archive already exists")
    if not source.is_dir() or source.is_symlink():
        raise RCError("extension bundle is missing or unsafe")
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_name(f".{output.name}.tmp")
    try:
        with temporary.open("wb") as output_stream:
            with gzip.GzipFile(filename="", mode="wb", fileobj=output_stream, mtime=0) as compressed:
                with tarfile.open(fileobj=compressed, mode="w|", format=tarfile.PAX_FORMAT) as archive:
                    for path in sorted(source.rglob("*"), key=lambda item: item.relative_to(source).as_posix()):
                        if path.is_symlink():
                            raise RCError("extension bundle contains a symbolic link")
                        relative = Path("extension") / path.relative_to(source)
                        info = archive.gettarinfo(str(path), arcname=relative.as_posix())
                        info.uid = 0
                        info.gid = 0
                        info.uname = ""
                        info.gname = ""
                        info.mtime = 0
                        if path.is_file():
                            with path.open("rb") as stream:
                                archive.addfile(info, stream)
                        elif path.is_dir():
                            archive.addfile(info)
                        else:
                            raise RCError("extension bundle contains an unsupported entry")
        os.replace(temporary, output)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise


def load_json(path: Path, description: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise RCError(f"{description} is missing or malformed: {error}") from error
    if not isinstance(value, dict):
        raise RCError(f"{description} must be an object")
    return value


def write_manifest(candidate_dir: Path, candidate_id: str, source_commit: str, cells_dir: Path, smoke_summary: Path, pg_receipt: Path) -> None:
    artifacts = verify(candidate_dir, candidate_id, source_commit)
    if not COMMIT.fullmatch(source_commit):
        raise RCError("source commit must be a full Git commit hash")
    summary = load_json(smoke_summary, "packaged smoke summary")
    if summary.get("source_commit") != source_commit or summary.get("status") != "passed":
        raise RCError("packaged smoke summary did not pass for the candidate commit")
    obligations = summary.get("obligations")
    if not isinstance(obligations, list) or not obligations:
        raise RCError("packaged smoke summary contains no obligations")
    for obligation in obligations:
        if not isinstance(obligation, dict) or obligation.get("status") != "passed":
            raise RCError("packaged smoke summary contains a failed obligation")
        count = obligation.get("test_count")
        if obligation.get("terminal") is not True or not isinstance(count, int) or isinstance(count, bool) or count < 1:
            raise RCError("packaged smoke summary contains an unexecuted or skipped obligation")
    artifact_hashes = {record["sha256"] for record in artifacts}
    summary_hashes = summary.get("artifact_hashes")
    if not isinstance(summary_hashes, list) or not summary_hashes or not set(summary_hashes).issubset(artifact_hashes):
        raise RCError("packaged smoke evidence references an unstaged artifact")
    cell_ids = sorted({str(item["id"]).split("/")[1] for item in obligations})
    cells: list[dict[str, Any]] = []
    for cell_id in cell_ids:
        cell_path = cells_dir / f"{cell_id}.json"
        cell = load_json(cell_path, f"support cell {cell_id}")
        if cell.get("cell_id") != cell_id or cell.get("source_commit") != source_commit or cell.get("status") != "passed":
            raise RCError(f"support cell {cell_id} did not pass for the candidate commit")
        hashes = cell.get("artifact_hashes")
        if not isinstance(hashes, list) or not hashes or not set(hashes).issubset(artifact_hashes):
            raise RCError(f"support cell {cell_id} references an unstaged artifact")
        cells.append({"support_cell_id": cell_id, "evidence_path": cell_path.relative_to(candidate_dir).as_posix(), "artifact_hashes": hashes})
    receipt = load_json(pg_receipt, "PostgreSQL 18 gate receipt")
    receipt_count = receipt.get("test_count")
    if receipt.get("gate") != "rc-check-pg18" or receipt.get("source_commit") != source_commit or receipt.get("status") != "passed" or receipt.get("terminal") is not True or not isinstance(receipt_count, int) or isinstance(receipt_count, bool) or receipt_count < 1:
        raise RCError("PostgreSQL 18 gate receipt is not a terminal pass")
    manifest = {
        "schema_version": 1,
        "candidate_id": candidate_id,
        "release_version": "0.3.0",
        "source_commit": source_commit,
        "checksums": CHECKSUMS_NAME,
        "artifacts": artifacts,
        "executed_cells": cells,
        "gate_receipts": [{"gate": "rc-check-pg18", "path": pg_receipt.relative_to(candidate_dir).as_posix(), "test_count": receipt_count}],
    }
    output = candidate_dir / MANIFEST_NAME
    if output.exists():
        raise RCError("RC manifest already exists")
    write_atomic(output, json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    verify(candidate_dir, candidate_id, source_commit)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    archive_parser = subparsers.add_parser("archive-extension")
    archive_parser.add_argument("--source", type=Path, required=True)
    archive_parser.add_argument("--output", type=Path, required=True)
    for command in ("seal", "verify"):
        subparser = subparsers.add_parser(command)
        subparser.add_argument("--candidate-dir", type=Path, required=True)
        subparser.add_argument("--candidate-id", required=True)
        subparser.add_argument("--source-commit")
    validate_parser = subparsers.add_parser("validate-id")
    validate_parser.add_argument("--candidate-id", required=True)
    run_parser = subparsers.add_parser("run-verified")
    run_parser.add_argument("--candidate-dir", type=Path, required=True)
    run_parser.add_argument("--candidate-id", required=True)
    run_parser.add_argument("--source-commit", required=True)
    run_parser.add_argument("execution", nargs=argparse.REMAINDER)
    manifest_parser = subparsers.add_parser("manifest")
    manifest_parser.add_argument("--candidate-dir", type=Path, required=True)
    manifest_parser.add_argument("--candidate-id", required=True)
    manifest_parser.add_argument("--source-commit", required=True)
    manifest_parser.add_argument("--cells-dir", type=Path, required=True)
    manifest_parser.add_argument("--smoke-summary", type=Path, required=True)
    manifest_parser.add_argument("--pg-receipt", type=Path, required=True)
    args = parser.parse_args()
    try:
        if args.command == "archive-extension":
            archive_extension(args.source, args.output)
        elif args.command == "validate-id":
            if not CANDIDATE.fullmatch(args.candidate_id):
                raise RCError("candidate ID must match RC-0.3.0-YYYYMMDDTHHMMSSZ-<commit>")
        elif args.command == "seal":
            if args.source_commit is None:
                parser.error("seal requires --source-commit")
            seal(args.candidate_dir, args.candidate_id, args.source_commit)
        elif args.command == "verify":
            verify(args.candidate_dir, args.candidate_id, args.source_commit)
        elif args.command == "run-verified":
            execution = args.execution[1:] if args.execution[:1] == ["--"] else args.execution
            return run_verified(args.candidate_dir, args.candidate_id, args.source_commit, execution)
        else:
            write_manifest(args.candidate_dir, args.candidate_id, args.source_commit, args.cells_dir, args.smoke_summary, args.pg_receipt)
    except RCError as error:
        parser.error(str(error))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
