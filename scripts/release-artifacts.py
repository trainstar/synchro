#!/usr/bin/env python3
"""Assemble, seal, and verify one exact Synchro release distribution."""

from __future__ import annotations

import argparse
import base64
import binascii
import gzip
import hashlib
import json
import os
import re
import shutil
import stat
import subprocess
import tarfile
import tempfile
import zipfile
import xml.etree.ElementTree as ElementTree
from pathlib import Path, PurePosixPath
from typing import Any, Iterable


VERSION = re.compile(r"^[0-9]+\.[0-9]+\.[0-9]+$")
COMMIT = re.compile(r"^[0-9a-f]{40}$")
SHA256 = re.compile(r"^[0-9a-f]{64}$")
RUN_ID = re.compile(r"^[1-9][0-9]*$")
REPOSITORY = "trainstar/synchro"
PROVENANCE_ISSUER = "https://token.actions.githubusercontent.com"
CANDIDATE_WORKFLOW = ".github/workflows/ci.yml"
BUILD_WORKFLOW = ".github/workflows/release.yml"
MANIFEST_NAME = "release-manifest.json"
CHECKSUMS_NAME = "SHA256SUMS"
SBOM_NAME = "sbom.spdx.json"
SERVER_METADATA_NAME = "server-metadata.json"
PACKAGE_METADATA_NAME = "package-metadata.json"
DEPENDENCY_INPUTS = (
    "Package.swift",
    "Synchro.podspec",
    "api/go/go.mod",
    "api/go/go.sum",
    "clients/kotlin/build.gradle.kts",
    "clients/kotlin/gradle.properties",
    "clients/kotlin/gradle/wrapper/gradle-wrapper.properties",
    "clients/kotlin/settings.gradle.kts",
    "clients/kotlin/synchro/build.gradle.kts",
    "clients/react-native/SynchroReactNative.podspec",
    "clients/react-native/android/build.gradle",
    "clients/react-native/package.json",
    "clients/react-native/yarn.lock",
    "extensions/Cargo.lock",
    "extensions/Cargo.toml",
    "extensions/synchro-pg/Cargo.toml",
)


class ReleaseError(ValueError):
    """Describe one fail-closed release artifact error."""


def load_json(path: Path, description: str) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ReleaseError(f"{description} is missing or malformed: {error}") from error


def write_atomic(path: Path, data: str, mode: int = 0o644) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, mode)
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError as error:
        raise ReleaseError(f"cannot hash {path}: {error}") from error
    return digest.hexdigest()


def safe_relative(value: str, description: str) -> PurePosixPath:
    path = PurePosixPath(value)
    if not value or path.is_absolute() or ".." in path.parts or "." in path.parts or "\\" in value:
        raise ReleaseError(f"{description} is unsafe: {value}")
    return path


def render(template: str, version: str) -> str:
    if template.count("{version}") != 1:
        raise ReleaseError(f"artifact template must contain one {{version}} field: {template}")
    return template.replace("{version}", version)


def validate_candidate(release_dir: Path, version: str, source_commit: str) -> str:
    if not VERSION.fullmatch(version):
        raise ReleaseError("release version must match X.Y.Z")
    if not COMMIT.fullmatch(source_commit):
        raise ReleaseError("source commit must be a full Git SHA-1")
    candidate_id = f"release-{version}-{source_commit}"
    if release_dir.name != candidate_id:
        raise ReleaseError(f"release directory name must be {candidate_id}")
    return candidate_id


def load_inventory(path: Path, version: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    value = load_json(path, "artifact inventory")
    if not isinstance(value, dict) or value.get("schema_version") != 1 or value.get("release") != version:
        raise ReleaseError("artifact inventory does not match the release version")
    artifacts = value.get("artifacts")
    if not isinstance(artifacts, list) or not artifacts:
        raise ReleaseError("artifact inventory contains no artifacts")
    public = []
    seen_ids: set[str] = set()
    seen_files: set[str] = set()
    for artifact in artifacts:
        if not isinstance(artifact, dict) or not isinstance(artifact.get("id"), str):
            raise ReleaseError("artifact inventory contains a malformed entry")
        if artifact["id"] in seen_ids:
            raise ReleaseError(f"artifact inventory repeats {artifact['id']}")
        seen_ids.add(artifact["id"])
        if artifact.get("visibility") == "internal":
            continue
        if artifact.get("visibility") != "public" or artifact.get("kind") not in {"file", "source"}:
            raise ReleaseError(f"artifact inventory entry {artifact['id']} has invalid publication data")
        if artifact["kind"] == "file":
            for field in ("stage_root", "staging_path_template", "release_path_template", "destination_template"):
                if not isinstance(artifact.get(field), str):
                    raise ReleaseError(f"artifact inventory entry {artifact['id']} lacks {field}")
            release_path = render(artifact["release_path_template"], version)
            safe_relative(release_path, "release artifact path")
            if release_path in seen_files:
                raise ReleaseError(f"artifact inventory repeats release path {release_path}")
            seen_files.add(release_path)
        else:
            for field in ("source_path", "source_tag_template", "destination_template"):
                if not isinstance(artifact.get(field), str):
                    raise ReleaseError(f"artifact inventory entry {artifact['id']} lacks {field}")
            safe_relative(artifact["source_path"], "source distribution path")
            render(artifact["source_tag_template"], version)
        public.append(artifact)
    if not public:
        raise ReleaseError("artifact inventory contains no public distributions")
    return artifacts, public


def regular_files(root: Path) -> list[str]:
    if not root.is_dir() or root.is_symlink():
        raise ReleaseError(f"staging directory is missing or unsafe: {root}")
    files: list[str] = []
    for current, directories, names in os.walk(root, followlinks=False):
        current_path = Path(current)
        for name in directories:
            path = current_path / name
            if path.is_symlink():
                raise ReleaseError(f"staging directory contains a symbolic link: {path.relative_to(root)}")
        for name in names:
            path = current_path / name
            relative = path.relative_to(root).as_posix()
            if path.is_symlink() or not path.is_file():
                raise ReleaseError(f"staging directory contains an unsupported entry: {relative}")
            files.append(relative)
    return sorted(files)


def validate_exact_stage(root: Path, expected: set[str]) -> None:
    actual = set(regular_files(root))
    if actual != expected:
        missing = sorted(expected - actual)
        extra = sorted(actual - expected)
        raise ReleaseError(f"staged artifact set is incomplete or unexpected: missing={missing}, extra={extra}")


def validate_elf_x64(path: Path) -> None:
    try:
        header = path.read_bytes()[:20]
    except OSError as error:
        raise ReleaseError(f"cannot read Linux executable {path}: {error}") from error
    if len(header) < 20 or header[:4] != b"\x7fELF" or header[4] != 2 or header[5] != 1:
        raise ReleaseError(f"Linux executable is not little-endian ELF64: {path}")
    if int.from_bytes(header[18:20], "little") != 62:
        raise ReleaseError(f"Linux executable is not x86-64: {path}")
    if not path.stat().st_mode & stat.S_IXUSR:
        raise ReleaseError(f"Linux executable is not executable: {path}")


def archive_extension(source: Path, output: Path) -> None:
    if output.exists():
        raise ReleaseError("extension archive already exists")
    files = regular_files(source)
    if not files:
        raise ReleaseError("extension bundle contains no files")
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_name(f".{output.name}.tmp")
    try:
        with temporary.open("wb") as output_stream:
            with gzip.GzipFile(filename="", mode="wb", fileobj=output_stream, mtime=0) as compressed:
                with tarfile.open(fileobj=compressed, mode="w|", format=tarfile.PAX_FORMAT) as archive:
                    for relative in files:
                        path = source / relative
                        info = archive.gettarinfo(str(path), arcname=f"extension/{relative}")
                        info.uid = info.gid = 0
                        info.uname = info.gname = ""
                        info.mtime = 0
                        with path.open("rb") as stream:
                            archive.addfile(info, stream)
        os.replace(temporary, output)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise


def archive_file_map(path: Path, kind: str) -> dict[str, bytes]:
    result: dict[str, bytes] = {}
    try:
        with tarfile.open(path, "r:gz") as archive:
            for member in archive.getmembers():
                try:
                    safe_relative(member.name, f"{kind} archive path")
                except ReleaseError as error:
                    raise ReleaseError(f"{kind} archive contains an unsafe path") from error
                if member.issym() or member.islnk():
                    raise ReleaseError(f"{kind} archive contains a link")
                if member.isfile():
                    stream = archive.extractfile(member)
                    if stream is None or member.name in result:
                        raise ReleaseError(f"{kind} archive contains an invalid file")
                    result[member.name] = stream.read()
                else:
                    raise ReleaseError(f"{kind} archive contains an unsupported entry")
    except (OSError, tarfile.TarError) as error:
        raise ReleaseError(f"{kind} archive is invalid: {error}") from error
    return result


def validate_extension_archive(path: Path, version: str) -> None:
    files = archive_file_map(path, "extension")
    prefix = "extension/"
    if not files or any(not name.startswith(prefix) for name in files):
        raise ReleaseError("extension archive paths must use the extension root")
    manifest_name = prefix + "artifact-manifest.json"
    checksum_name = manifest_name + ".sha256"
    try:
        manifest = json.loads(files[manifest_name])
    except (KeyError, json.JSONDecodeError, UnicodeDecodeError) as error:
        raise ReleaseError(f"extension archive manifest is missing or malformed: {error}") from error
    if set(manifest) != {"format", "postgresql_major", "postgresql_version", "files"}:
        raise ReleaseError("extension archive manifest has unexpected fields")
    if manifest["format"] != "synchro-pg18-extension-bundle-v1" or manifest["postgresql_major"] != 18:
        raise ReleaseError("extension archive does not target PostgreSQL 18")
    records = manifest.get("files")
    if not isinstance(records, list) or len(records) != 3:
        raise ReleaseError("extension archive must contain one library, control file, and install SQL")
    expected = {manifest_name, checksum_name}
    destinations: set[str] = set()
    for record in records:
        if not isinstance(record, dict) or set(record) != {"path", "destination", "sha256"}:
            raise ReleaseError("extension archive contains a malformed file record")
        relative = safe_relative(str(record["path"]), "extension archive manifest path").as_posix()
        archive_name = prefix + relative
        if archive_name in expected or archive_name not in files or not SHA256.fullmatch(str(record["sha256"])):
            raise ReleaseError("extension archive contains an invalid file record")
        if hashlib.sha256(files[archive_name]).hexdigest() != record["sha256"]:
            raise ReleaseError("extension archive file hash does not match its manifest")
        expected.add(archive_name)
        destinations.add(str(record["destination"]))
    wanted_destinations = {
        "pkglibdir/synchro_pg.so",
        "sharedir/extension/synchro_pg.control",
        f"sharedir/extension/synchro_pg--{version}.sql",
    }
    if destinations != wanted_destinations or set(files) != expected:
        raise ReleaseError("extension archive content set does not match the PostgreSQL 18 distribution")
    expected_manifest_hash = hashlib.sha256(files[manifest_name]).hexdigest()
    if files[checksum_name].decode("ascii", "strict").strip() != expected_manifest_hash:
        raise ReleaseError("extension archive manifest checksum is invalid")
    control_record = next(record for record in records if record["destination"].endswith("synchro_pg.control"))
    control = files[prefix + control_record["path"]].decode("utf-8", "strict")
    if f"default_version = '{version}'" not in control:
        raise ReleaseError("extension archive control version is wrong")
    library_record = next(record for record in records if record["destination"] == "pkglibdir/synchro_pg.so")
    library = files[prefix + library_record["path"]]
    if len(library) < 20 or library[:6] != b"\x7fELF\x02\x01" or int.from_bytes(library[18:20], "little") != 62:
        raise ReleaseError("extension archive library is not Linux x86-64 ELF")


def maven_payload_paths(version: str) -> tuple[str, list[str]]:
    base = f"fit/trainstar/synchro/{version}/synchro-{version}"
    return base, [f"{base}.pom", f"{base}.aar", f"{base}-sources.jar", f"{base}-javadoc.jar"]


def crc24(data: bytes) -> bytes:
    value = 0xB704CE
    for octet in data:
        value ^= octet << 16
        for _ in range(8):
            value <<= 1
            if value & 0x1000000:
                value ^= 0x1864CFB
    return (value & 0xFFFFFF).to_bytes(3, "big")


def validate_armored_signature(path: Path) -> None:
    try:
        lines = path.read_text(encoding="ascii").splitlines()
    except (OSError, UnicodeDecodeError) as error:
        raise ReleaseError(f"Maven detached signature is unreadable: {path}") from error
    if len(lines) < 5 or lines[0] != "-----BEGIN PGP SIGNATURE-----" or lines[-1] != "-----END PGP SIGNATURE-----":
        raise ReleaseError(f"Maven detached signature is not armored: {path}")
    try:
        separator = lines.index("")
    except (ValueError, binascii.Error) as error:
        raise ReleaseError(f"Maven detached signature lacks an armor separator: {path}") from error
    if any(":" not in line for line in lines[1:separator]):
        raise ReleaseError(f"Maven detached signature has an invalid armor header: {path}")
    content = lines[separator + 1:-1]
    checksum_lines = [line for line in content if line.startswith("=")]
    encoded = [line for line in content if not line.startswith("=")]
    if len(checksum_lines) != 1 or not encoded or content[-1] != checksum_lines[0]:
        raise ReleaseError(f"Maven detached signature has an invalid armor body: {path}")
    try:
        packet = base64.b64decode("".join(encoded), validate=True)
        checksum = base64.b64decode(checksum_lines[0][1:], validate=True)
    except ValueError as error:
        raise ReleaseError(f"Maven detached signature has invalid base64: {path}") from error
    if len(packet) < 8 or not packet[0] & 0x80:
        raise ReleaseError(f"Maven detached signature lacks an OpenPGP packet: {path}")
    packet_tag = packet[0] & 0x3F if packet[0] & 0x40 else (packet[0] >> 2) & 0x0F
    if packet_tag != 2 or checksum != crc24(packet):
        raise ReleaseError(f"Maven detached signature armor is invalid: {path}")


def validate_signed_maven_payloads(root: Path, version: str) -> list[str]:
    files = regular_files(root)
    base, payloads = maven_payload_paths(version)
    names = set(files)
    for payload in payloads:
        if payload not in names or payload + ".asc" not in names:
            raise ReleaseError(f"Maven release bundle lacks payload or detached signature: {payload}")
        validate_armored_signature(root / (payload + ".asc"))
    try:
        pom = ElementTree.parse(root / f"{base}.pom").getroot()
    except (OSError, ElementTree.ParseError) as error:
        raise ReleaseError(f"Maven release bundle POM is invalid: {error}") from error
    pom_identity = tuple((pom.find(f"{{*}}{field}").text or "") if pom.find(f"{{*}}{field}") is not None else "" for field in ("groupId", "artifactId", "version"))
    if pom_identity != ("fit.trainstar", "synchro", version):
        raise ReleaseError("Maven release bundle POM coordinates or version are wrong")
    artifact_root = "fit/trainstar/synchro/"
    version_file = re.compile(
        rf"^{re.escape(base)}(?:\.aar|\.pom|\.module|-sources\.jar|-javadoc\.jar)(?:\.asc)?(?:\.(?:md5|sha1|sha256|sha512))?$"
    )
    metadata_file = re.compile(r"^fit/trainstar/synchro/maven-metadata\.xml(?:\.(?:md5|sha1|sha256|sha512))?$")
    if any(not name.startswith(artifact_root) or not (version_file.fullmatch(name) or metadata_file.fullmatch(name)) for name in files):
        raise ReleaseError("Maven release bundle contains an unexpected coordinate entry")
    return files


def prepare_maven_repository(root: Path, version: str) -> None:
    files = validate_signed_maven_payloads(root, version)
    checksum_suffixes = (".md5", ".sha1", ".sha256", ".sha512")
    originals = [relative for relative in files if not relative.endswith(checksum_suffixes)]
    for relative in files:
        if relative.endswith(checksum_suffixes):
            (root / relative).unlink()
    for relative in originals:
        data = (root / relative).read_bytes()
        for suffix, algorithm in ((".md5", "md5"), (".sha1", "sha1")):
            write_atomic(root / f"{relative}{suffix}", hashlib.new(algorithm, data).hexdigest() + "\n")


def maven_repository_files(root: Path, version: str) -> list[str]:
    files = validate_signed_maven_payloads(root, version)
    checksum_suffixes = (".md5", ".sha1", ".sha256", ".sha512")
    originals = [relative for relative in files if not relative.endswith(checksum_suffixes)]
    expected = set(originals)
    for relative in originals:
        data = (root / relative).read_bytes()
        for suffix, algorithm in ((".md5", "md5"), (".sha1", "sha1")):
            checksum = f"{relative}{suffix}"
            expected.add(checksum)
            if checksum not in files:
                raise ReleaseError(f"Maven release bundle lacks generated checksum: {checksum}")
            value = (root / checksum).read_text(encoding="ascii").strip()
            if value != hashlib.new(algorithm, data).hexdigest() or value != value.lower():
                raise ReleaseError(f"Maven release bundle checksum is invalid: {checksum}")
    if set(files) != expected:
        raise ReleaseError("Maven release bundle contains a noncanonical checksum set")
    return files


def archive_maven(source: Path, output: Path, version: str) -> None:
    files = maven_repository_files(source, version)
    if output.exists():
        raise ReleaseError("Maven archive already exists")
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_name(f".{output.name}.tmp")
    try:
        with zipfile.ZipFile(temporary, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as archive:
            for relative in files:
                info = zipfile.ZipInfo(relative, date_time=(1980, 1, 1, 0, 0, 0))
                info.compress_type = zipfile.ZIP_DEFLATED
                info.external_attr = 0o100644 << 16
                archive.writestr(info, (source / relative).read_bytes())
        os.replace(temporary, output)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise


def validate_maven_archive(path: Path, version: str) -> None:
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        try:
            with zipfile.ZipFile(path) as archive:
                seen: set[str] = set()
                for info in archive.infolist():
                    relative = safe_relative(info.filename, "Maven archive path").as_posix()
                    mode = info.external_attr >> 16
                    if stat.S_ISLNK(mode) or info.is_dir() or relative in seen:
                        raise ReleaseError("Maven archive contains a link, directory, or duplicate entry")
                    seen.add(relative)
                    destination = root / relative
                    destination.parent.mkdir(parents=True, exist_ok=True)
                    destination.write_bytes(archive.read(info))
        except (OSError, zipfile.BadZipFile) as error:
            raise ReleaseError(f"Maven archive is invalid: {error}") from error
        maven_repository_files(root, version)


def validate_npm_archive(path: Path, version: str) -> None:
    files = archive_file_map(path, "npm")
    try:
        package = json.loads(files["package/package.json"])
    except (KeyError, json.JSONDecodeError, UnicodeDecodeError) as error:
        raise ReleaseError(f"npm archive package metadata is missing or malformed: {error}") from error
    if package.get("name") != "@trainstar/synchro-react-native" or package.get("version") != version:
        raise ReleaseError("npm archive package name or version is wrong")
    if any(not name.startswith("package/") for name in files):
        raise ReleaseError("npm archive paths must use the package root")


def dependency_output(path: Path) -> dict[str, Any]:
    try:
        process = subprocess.run(["ldd", str(path)], text=True, capture_output=True, check=False)
    except OSError as error:
        raise ReleaseError(f"cannot inspect Linux binary dependencies: {error}") from error
    output = (process.stdout + process.stderr).splitlines()
    return {"tool": "ldd", "exit_code": process.returncode, "output": output}


def write_server_metadata(output: Path, source_commit: str, binaries: list[tuple[str, Path]]) -> None:
    if not COMMIT.fullmatch(source_commit) or len(binaries) != 2:
        raise ReleaseError("server metadata requires one source commit and two binaries")
    records = []
    for staging_path, path in sorted(binaries):
        safe_relative(staging_path, "server binary staging path")
        validate_elf_x64(path)
        records.append({
            "staging_path": staging_path,
            "format": "ELF64",
            "machine": "x86-64",
            "dependencies": dependency_output(path),
        })
    write_atomic(output, json.dumps({"schema_version": 1, "source_commit": source_commit, "binaries": records}, indent=2, sort_keys=True) + "\n")


def validate_server_metadata(path: Path, source_commit: str, executable_paths: set[str]) -> dict[str, dict[str, Any]]:
    value = load_json(path, "server metadata")
    if not isinstance(value, dict) or set(value) != {"schema_version", "source_commit", "binaries"}:
        raise ReleaseError("server metadata has unexpected fields")
    if value["schema_version"] != 1 or value["source_commit"] != source_commit or not isinstance(value["binaries"], list):
        raise ReleaseError("server metadata has invalid source identity")
    result: dict[str, dict[str, Any]] = {}
    for record in value["binaries"]:
        if not isinstance(record, dict) or set(record) != {"staging_path", "format", "machine", "dependencies"}:
            raise ReleaseError("server metadata contains a malformed binary record")
        relative = str(record["staging_path"])
        dependencies = record["dependencies"]
        if record["format"] != "ELF64" or record["machine"] != "x86-64":
            raise ReleaseError("server metadata contains a wrong binary identity")
        if not isinstance(dependencies, dict) or set(dependencies) != {"tool", "exit_code", "output"}:
            raise ReleaseError("server metadata contains malformed dependency output")
        if dependencies["tool"] != "ldd" or not isinstance(dependencies["exit_code"], int) or not isinstance(dependencies["output"], list) or not all(isinstance(line, str) for line in dependencies["output"]):
            raise ReleaseError("server metadata contains invalid dependency output")
        if relative in result:
            raise ReleaseError("server metadata repeats a binary")
        result[relative] = record
    if set(result) != executable_paths:
        raise ReleaseError("server metadata does not cover the exact Linux executable set")
    return result


def write_package_metadata(output: Path, source_commit: str) -> None:
    if not COMMIT.fullmatch(source_commit):
        raise ReleaseError("package metadata requires a full source commit")
    write_atomic(output, json.dumps({"schema_version": 1, "source_commit": source_commit}, indent=2, sort_keys=True) + "\n")


def validate_package_metadata(path: Path, source_commit: str) -> None:
    value = load_json(path, "package metadata")
    if value != {"schema_version": 1, "source_commit": source_commit}:
        raise ReleaseError("package components do not match the release source commit")


def required_support_cells(path: Path) -> set[str]:
    value = load_json(path, "support matrix")
    cells = value.get("cells") if isinstance(value, dict) else None
    if not isinstance(cells, list):
        raise ReleaseError("support matrix does not contain cells")
    return {str(cell["id"]) for cell in cells if isinstance(cell, dict) and cell.get("policy") == "required"}


def load_support_resolution(path: Path, matrix_path: Path) -> list[dict[str, Any]]:
    value = load_json(path, "resolved support environments")
    if not isinstance(value, list):
        raise ReleaseError("resolved support environments must be an array")
    required = required_support_cells(matrix_path)
    seen: set[str] = set()
    for record in value:
        if not isinstance(record, dict) or set(record) != {"id", "environment"} or not isinstance(record["environment"], dict) or not record["environment"]:
            raise ReleaseError("resolved support environments contain a malformed record")
        if record["id"] in seen or not all(isinstance(key, str) and isinstance(item, str) and item for key, item in record["environment"].items()):
            raise ReleaseError("resolved support environments contain a duplicate or empty value")
        if any(re.search(r"(?:current|latest|stable|(?:^|\.)x(?:\.|$)|\*)", item, re.IGNORECASE) for item in record["environment"].values()):
            raise ReleaseError("resolved support environments contain an unresolved selector")
        seen.add(record["id"])
    if seen != required:
        raise ReleaseError("resolved support environments do not match the support matrix")
    return sorted(value, key=lambda record: record["id"])


def validate_sbom(path: Path, payload_hashes: set[str]) -> None:
    value = load_json(path, "SPDX JSON SBOM")
    if not isinstance(value, dict) or value.get("spdxVersion") not in {"SPDX-2.2", "SPDX-2.3"}:
        raise ReleaseError("SBOM is not SPDX JSON")
    if value.get("SPDXID") != "SPDXRef-DOCUMENT" or value.get("dataLicense") != "CC0-1.0":
        raise ReleaseError("SBOM lacks the required SPDX document identity")
    creation = value.get("creationInfo")
    if not isinstance(value.get("name"), str) or not value["name"] or not isinstance(value.get("documentNamespace"), str) or not value["documentNamespace"]:
        raise ReleaseError("SBOM lacks required SPDX document metadata")
    if not isinstance(creation, dict) or not isinstance(creation.get("created"), str) or not creation["created"] or not isinstance(creation.get("creators"), list) or not creation["creators"] or not all(isinstance(creator, str) and creator for creator in creation["creators"]):
        raise ReleaseError("SBOM lacks required SPDX creation metadata")
    files = value.get("files")
    if not isinstance(files, list):
        raise ReleaseError("SBOM does not contain SPDX file records")
    hashes: set[str] = set()
    for record in files:
        if not isinstance(record, dict) or not isinstance(record.get("fileName"), str):
            raise ReleaseError("SBOM contains a malformed SPDX file record")
        checksums = record.get("checksums")
        if not isinstance(checksums, list):
            raise ReleaseError("SBOM file record lacks checksums")
        for checksum in checksums:
            if isinstance(checksum, dict) and checksum.get("algorithm") == "SHA256" and SHA256.fullmatch(str(checksum.get("checksumValue", "")).lower()):
                hashes.add(str(checksum["checksumValue"]).lower())
    if not payload_hashes.issubset(hashes):
        raise ReleaseError("SBOM does not include every public payload hash")


def distribution_records(public: list[dict[str, Any]], version: str, release_dir: Path, source_trees: dict[str, str], server_metadata: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    records = []
    for artifact in public:
        common = {
            "inventory_id": artifact["id"],
            "role": artifact["role"],
            "name": artifact["name"],
            "kind": artifact["kind"],
            "destination": render(artifact["destination_template"], version),
        }
        if artifact["kind"] == "file":
            relative = render(artifact["release_path_template"], version)
            path = release_dir / relative
            record = {**common, "path": relative, "size_bytes": path.stat().st_size, "sha256": file_sha256(path)}
            stage_path = render(artifact["staging_path_template"], version)
            if stage_path in server_metadata:
                record["binary_identity"] = {
                    "format": "ELF64",
                    "machine": "x86-64",
                    "dependencies": server_metadata[stage_path]["dependencies"],
                }
            records.append(record)
        else:
            source_path = artifact["source_path"]
            tree = source_trees.get(source_path)
            if not tree or not COMMIT.fullmatch(tree):
                raise ReleaseError(f"source tree identity is missing for {source_path}")
            records.append({**common, "source_path": source_path, "source_tag": render(artifact["source_tag_template"], version), "git_tree": tree})
    return records


def validate_payload(path: Path, role: str, version: str) -> None:
    if role == "pg-extension":
        validate_extension_archive(path, version)
    elif role in {"adapter", "seed-tool"}:
        validate_elf_x64(path)
    elif role == "kotlin-maven":
        validate_maven_archive(path, version)
    elif role == "react-native-npm":
        validate_npm_archive(path, version)


def parse_source_trees(values: Iterable[str]) -> dict[str, str]:
    result: dict[str, str] = {}
    for value in values:
        source_path, separator, tree = value.partition("=")
        if not separator or source_path in result or not COMMIT.fullmatch(tree):
            raise ReleaseError("source tree must use unique path=<full Git tree SHA> values")
        safe_relative(source_path, "source tree path")
        result[source_path] = tree
    return result


def stage_release(*, release_dir: Path, version: str, source_commit: str, inventory_path: Path, support_matrix: Path, support_resolution: Path, server_dir: Path, packages_dir: Path, sbom: Path, repo_root: Path, source_trees: dict[str, str], candidate_ci_run_id: str, candidate_ci_run_attempt: int, build_run_id: str, build_run_attempt: int) -> None:
    candidate_id = validate_candidate(release_dir, version, source_commit)
    if release_dir.exists():
        raise ReleaseError("release directory already exists")
    if not RUN_ID.fullmatch(candidate_ci_run_id) or candidate_ci_run_attempt < 1:
        raise ReleaseError("candidate CI run identity is invalid")
    if not RUN_ID.fullmatch(build_run_id) or build_run_attempt < 1:
        raise ReleaseError("release build run identity is invalid")
    if candidate_ci_run_id == build_run_id:
        raise ReleaseError("candidate CI and release build must use different workflow runs")
    _, public = load_inventory(inventory_path, version)
    file_artifacts = [artifact for artifact in public if artifact["kind"] == "file"]
    expected_server = {render(item["staging_path_template"], version) for item in file_artifacts if item["stage_root"] == "server"}
    expected_packages = {render(item["staging_path_template"], version) for item in file_artifacts if item["stage_root"] == "packages"}
    validate_exact_stage(server_dir, expected_server | {SERVER_METADATA_NAME})
    validate_exact_stage(packages_dir, expected_packages | {PACKAGE_METADATA_NAME})
    executable_paths = {render(item["staging_path_template"], version) for item in file_artifacts if item["role"] in {"adapter", "seed-tool"}}
    metadata = validate_server_metadata(server_dir / SERVER_METADATA_NAME, source_commit, executable_paths)
    validate_package_metadata(packages_dir / PACKAGE_METADATA_NAME, source_commit)
    support = load_support_resolution(support_resolution, support_matrix)
    work = release_dir.with_name(f".{release_dir.name}.tmp.{os.getpid()}")
    if work.exists():
        raise ReleaseError("temporary release directory already exists")
    try:
        work.mkdir(parents=True)
        for artifact in file_artifacts:
            source_root = server_dir if artifact["stage_root"] == "server" else packages_dir
            source = source_root / render(artifact["staging_path_template"], version)
            destination = work / render(artifact["release_path_template"], version)
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source, destination)
            os.chmod(destination, source.stat().st_mode & 0o777)
            validate_payload(destination, artifact["role"], version)
        payload_hashes = {file_sha256(work / render(item["release_path_template"], version)) for item in file_artifacts}
        shutil.copyfile(sbom, work / SBOM_NAME)
        validate_sbom(work / SBOM_NAME, payload_hashes)
        dependencies = []
        for relative in DEPENDENCY_INPUTS:
            path = repo_root / relative
            if not path.is_file() or path.is_symlink():
                raise ReleaseError(f"dependency input is missing or unsafe: {relative}")
            dependencies.append({"path": relative, "size_bytes": path.stat().st_size, "sha256": file_sha256(path)})
        records = distribution_records(public, version, work, source_trees, metadata)
        source_tags = sorted({record["source_tag"] for record in records if record["kind"] == "source"})
        root_tags = sorted({record["source_tag"] for record in records if record.get("source_path") == "repo-root"})
        if root_tags != [f"v{version}"] or f"api/go/v{version}" not in source_tags:
            raise ReleaseError("artifact inventory does not declare the exact root and Go module tags")
        subjects = [{"name": record["path"], "sha256": record["sha256"]} for record in records if record["kind"] == "file"]
        subjects.append({"name": SBOM_NAME, "sha256": file_sha256(work / SBOM_NAME)})
        manifest = {
            "schema_version": 1,
            "candidate_id": candidate_id,
            "release_version": version,
            "source": {
                "repository": REPOSITORY,
                "commit": source_commit,
                "git_tree": source_trees.get("repo-root"),
                "root_tag": f"v{version}",
                "source_tags": source_tags,
            },
            "candidate_ci": {"run_id": candidate_ci_run_id, "run_attempt": candidate_ci_run_attempt, "workflow_path": CANDIDATE_WORKFLOW},
            "build": {"run_id": build_run_id, "run_attempt": build_run_attempt, "workflow_path": BUILD_WORKFLOW},
            "resolved_support_cells": support,
            "dependency_inputs": dependencies,
            "distributions": records,
            "sbom": {"path": SBOM_NAME, "format": "spdx-json", "size_bytes": (work / SBOM_NAME).stat().st_size, "sha256": file_sha256(work / SBOM_NAME)},
            "provenance": {
                "expected_issuer": PROVENANCE_ISSUER,
                "repository": REPOSITORY,
                "workflow_path": BUILD_WORKFLOW,
                "source_commit": source_commit,
                "subjects": sorted(subjects, key=lambda item: item["name"]),
            },
        }
        if not COMMIT.fullmatch(str(manifest["source"]["git_tree"] or "")):
            raise ReleaseError("root Git tree identity is missing")
        write_atomic(work / MANIFEST_NAME, json.dumps(manifest, indent=2, sort_keys=True) + "\n")
        shipped = sorted(regular_files(work))
        if CHECKSUMS_NAME in shipped:
            raise ReleaseError("checksum file unexpectedly exists before sealing")
        write_atomic(work / CHECKSUMS_NAME, "".join(f"{file_sha256(work / relative)}  {relative}\n" for relative in shipped))
        release_dir.parent.mkdir(parents=True, exist_ok=True)
        os.replace(work, release_dir)
        verify_release(release_dir, version, inventory_path, support_matrix, source_commit)
    except BaseException:
        shutil.rmtree(work, ignore_errors=True)
        if release_dir.exists():
            shutil.rmtree(release_dir, ignore_errors=True)
        raise


def load_checksum_records(release_dir: Path) -> dict[str, str]:
    try:
        lines = (release_dir / CHECKSUMS_NAME).read_text(encoding="utf-8").splitlines()
    except OSError as error:
        raise ReleaseError(f"SHA256SUMS is missing: {error}") from error
    records: dict[str, str] = {}
    for line in lines:
        digest, separator, relative = line.partition("  ")
        if not separator or not SHA256.fullmatch(digest) or relative in records:
            raise ReleaseError("SHA256SUMS is malformed")
        safe_relative(relative, "SHA256SUMS path")
        path = release_dir / relative
        if not path.is_file() or path.is_symlink() or file_sha256(path) != digest:
            raise ReleaseError(f"release file hash mismatch: {relative}")
        records[relative] = digest
    return records


def verify_release(release_dir: Path, version: str, inventory_path: Path, support_matrix: Path, source_commit: str | None = None) -> list[dict[str, Any]]:
    if not release_dir.is_dir() or release_dir.is_symlink():
        raise ReleaseError("release directory is missing or unsafe")
    manifest = load_json(release_dir / MANIFEST_NAME, "release manifest")
    if not isinstance(manifest, dict):
        raise ReleaseError("release manifest must be an object")
    expected_keys = {"schema_version", "candidate_id", "release_version", "source", "candidate_ci", "build", "resolved_support_cells", "dependency_inputs", "distributions", "sbom", "provenance"}
    if set(manifest) != expected_keys or manifest.get("schema_version") != 1 or manifest.get("release_version") != version:
        raise ReleaseError("release manifest shape or version is invalid")
    source = manifest.get("source")
    if not isinstance(source, dict) or set(source) != {"repository", "commit", "git_tree", "root_tag", "source_tags"}:
        raise ReleaseError("release manifest source identity is invalid")
    commit = str(source.get("commit", ""))
    validate_candidate(release_dir, version, commit)
    if source_commit is not None and commit != source_commit:
        raise ReleaseError("release source commit does not match the requested commit")
    if source["repository"] != REPOSITORY or source["root_tag"] != f"v{version}" or not COMMIT.fullmatch(str(source["git_tree"])):
        raise ReleaseError("release manifest source repository, tag, or tree is invalid")
    _, public = load_inventory(inventory_path, version)
    distributions = manifest.get("distributions")
    if not isinstance(distributions, list) or len(distributions) != len(public):
        raise ReleaseError("release manifest does not contain the complete public distribution set")
    by_id = {record.get("inventory_id"): record for record in distributions if isinstance(record, dict)}
    if len(by_id) != len(distributions):
        raise ReleaseError("release manifest repeats or malforms a distribution")
    expected_files = {MANIFEST_NAME, SBOM_NAME, CHECKSUMS_NAME}
    file_records: list[dict[str, Any]] = []
    source_tags: set[str] = set()
    subjects: list[dict[str, str]] = []
    for artifact in public:
        record = by_id.get(artifact["id"])
        if record is None:
            raise ReleaseError(f"release manifest lacks distribution {artifact['id']}")
        common = {"inventory_id": artifact["id"], "role": artifact["role"], "name": artifact["name"], "kind": artifact["kind"], "destination": render(artifact["destination_template"], version)}
        if any(record.get(key) != value for key, value in common.items()):
            raise ReleaseError(f"release distribution metadata is wrong for {artifact['id']}")
        if artifact["kind"] == "file":
            relative = render(artifact["release_path_template"], version)
            allowed = set(common) | {"path", "size_bytes", "sha256"}
            if artifact["role"] in {"adapter", "seed-tool"}:
                allowed.add("binary_identity")
            if set(record) != allowed or record.get("path") != relative:
                raise ReleaseError(f"release file record shape is wrong for {artifact['id']}")
            path = release_dir / relative
            if not path.is_file() or path.is_symlink() or record.get("size_bytes") != path.stat().st_size or record.get("sha256") != file_sha256(path):
                raise ReleaseError(f"release payload identity is wrong: {relative}")
            if artifact["role"] in {"adapter", "seed-tool"}:
                identity = record.get("binary_identity")
                if not isinstance(identity, dict) or set(identity) != {"format", "machine", "dependencies"} or identity["format"] != "ELF64" or identity["machine"] != "x86-64":
                    raise ReleaseError(f"Linux binary metadata is invalid: {relative}")
                dependencies_output = identity["dependencies"]
                if not isinstance(dependencies_output, dict) or set(dependencies_output) != {"tool", "exit_code", "output"} or dependencies_output.get("tool") != "ldd" or not isinstance(dependencies_output.get("exit_code"), int) or not isinstance(dependencies_output.get("output"), list) or not all(isinstance(line, str) for line in dependencies_output["output"]):
                    raise ReleaseError(f"Linux binary dependency output is invalid: {relative}")
            validate_payload(path, artifact["role"], version)
            expected_files.add(relative)
            file_records.append(record)
            subjects.append({"name": relative, "sha256": record["sha256"]})
        else:
            expected = {**common, "source_path": artifact["source_path"], "source_tag": render(artifact["source_tag_template"], version)}
            if set(record) != set(expected) | {"git_tree"} or any(record.get(key) != value for key, value in expected.items()) or not COMMIT.fullmatch(str(record.get("git_tree", ""))):
                raise ReleaseError(f"release source record is wrong for {artifact['id']}")
            source_tags.add(record["source_tag"])
    if source.get("source_tags") != sorted(source_tags) or source_tags != {f"v{version}", f"api/go/v{version}"}:
        raise ReleaseError("release source tags are incomplete or wrong")
    support_records = manifest.get("resolved_support_cells")
    if not isinstance(support_records, list) or {record.get("id") for record in support_records if isinstance(record, dict)} != required_support_cells(support_matrix):
        raise ReleaseError("release manifest support cells do not match the support matrix")
    for record in support_records:
        if set(record) != {"id", "environment"} or not isinstance(record["environment"], dict) or not record["environment"]:
            raise ReleaseError("release manifest contains malformed support resolution")
        if not all(isinstance(key, str) and isinstance(item, str) and item for key, item in record["environment"].items()):
            raise ReleaseError("release manifest contains an empty support resolution")
        if any(re.search(r"(?:current|latest|stable|(?:^|\.)x(?:\.|$)|\*)", item, re.IGNORECASE) for item in record["environment"].values()):
            raise ReleaseError("release manifest contains an unresolved support selector")
    dependencies = manifest.get("dependency_inputs")
    if not isinstance(dependencies, list) or [record.get("path") for record in dependencies if isinstance(record, dict)] != list(DEPENDENCY_INPUTS):
        raise ReleaseError("release manifest dependency inputs are incomplete")
    for record in dependencies:
        if set(record) != {"path", "size_bytes", "sha256"} or not isinstance(record["size_bytes"], int) or record["size_bytes"] < 0 or not SHA256.fullmatch(str(record["sha256"])):
            raise ReleaseError("release manifest contains a malformed dependency input")
    sbom = manifest.get("sbom")
    if not isinstance(sbom, dict) or set(sbom) != {"path", "format", "size_bytes", "sha256"} or sbom.get("path") != SBOM_NAME or sbom.get("format") != "spdx-json":
        raise ReleaseError("release manifest SBOM record is invalid")
    sbom_path = release_dir / SBOM_NAME
    if sbom.get("size_bytes") != sbom_path.stat().st_size or sbom.get("sha256") != file_sha256(sbom_path):
        raise ReleaseError("release manifest SBOM identity is invalid")
    validate_sbom(sbom_path, {record["sha256"] for record in file_records})
    subjects.append({"name": SBOM_NAME, "sha256": sbom["sha256"]})
    provenance = manifest.get("provenance")
    expected_provenance = {"expected_issuer": PROVENANCE_ISSUER, "repository": REPOSITORY, "workflow_path": BUILD_WORKFLOW, "source_commit": commit, "subjects": sorted(subjects, key=lambda item: item["name"])}
    if provenance != expected_provenance:
        raise ReleaseError("release manifest provenance expectation is invalid")
    for field, workflow in (("candidate_ci", CANDIDATE_WORKFLOW), ("build", BUILD_WORKFLOW)):
        run = manifest.get(field)
        if not isinstance(run, dict) or set(run) != {"run_id", "run_attempt", "workflow_path"} or not RUN_ID.fullmatch(str(run["run_id"])) or not isinstance(run["run_attempt"], int) or run["run_attempt"] < 1 or run["workflow_path"] != workflow:
            raise ReleaseError(f"release manifest {field} identity is invalid")
    if manifest["candidate_ci"]["run_id"] == manifest["build"]["run_id"]:
        raise ReleaseError("candidate CI and release build use the same workflow run")
    checksums = load_checksum_records(release_dir)
    actual_files = set(regular_files(release_dir))
    if actual_files != expected_files or set(checksums) != expected_files - {CHECKSUMS_NAME}:
        raise ReleaseError("sealed release contains a missing, renamed, or extra file")
    if MANIFEST_NAME not in checksums or CHECKSUMS_NAME in checksums:
        raise ReleaseError("SHA256SUMS creates a manifest or checksum circularity")
    return file_records


def run_verified(release_dir: Path, version: str, inventory: Path, support_matrix: Path, source_commit: str, command: list[str]) -> int:
    if not command:
        raise ReleaseError("verified execution requires a command")
    controls = (file_sha256(release_dir / MANIFEST_NAME), file_sha256(release_dir / CHECKSUMS_NAME))
    before = verify_release(release_dir, version, inventory, support_matrix, source_commit)
    status = subprocess.run(command, check=False).returncode
    after = verify_release(release_dir, version, inventory, support_matrix, source_commit)
    if before != after or controls != (file_sha256(release_dir / MANIFEST_NAME), file_sha256(release_dir / CHECKSUMS_NAME)):
        raise ReleaseError("release identity changed during execution")
    return status


def payload_records_by_role(release_dir: Path, version: str, inventory: Path, support_matrix: Path) -> dict[str, dict[str, Any]]:
    records = verify_release(release_dir, version, inventory, support_matrix)
    result = {str(record["role"]): record for record in records}
    if len(result) != len(records):
        raise ReleaseError("release contains duplicate file distribution roles")
    return result


def materialize_adapter_layout(release_dir: Path, version: str, inventory: Path, support_matrix: Path, output: Path) -> None:
    records = payload_records_by_role(release_dir, version, inventory, support_matrix)
    record = records.get("adapter")
    if record is None:
        raise ReleaseError("sealed release does not contain the adapter")
    if output.exists() and (not output.is_dir() or output.is_symlink() or any(output.iterdir())):
        raise ReleaseError("adapter layout output must be an empty safe directory")
    output.mkdir(parents=True, exist_ok=True)
    source = release_dir / record["path"]
    target = output / "synchrod-pg"
    shutil.copyfile(source, target)
    os.chmod(target, source.stat().st_mode & 0o777)
    if file_sha256(target) != record["sha256"]:
        raise ReleaseError("materialized adapter does not match the sealed manifest")
    write_atomic(output / "synchrod-pg.sha256", record["sha256"] + "\n")


def print_payload_hashes(release_dir: Path, version: str, inventory: Path, support_matrix: Path, roles: list[str]) -> None:
    records = payload_records_by_role(release_dir, version, inventory, support_matrix)
    if not roles or len(set(roles)) != len(roles):
        raise ReleaseError("payload hash roles must be nonempty and unique")
    try:
        print(" ".join(str(records[role]["sha256"]) for role in roles))
    except KeyError as error:
        raise ReleaseError(f"sealed release lacks payload role {error.args[0]}") from error


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    candidate = subparsers.add_parser("validate-id")
    candidate.add_argument("--release-dir", type=Path, required=True)
    candidate.add_argument("--version", required=True)
    candidate.add_argument("--source-commit", required=True)
    extension = subparsers.add_parser("archive-extension")
    extension.add_argument("--source", type=Path, required=True)
    extension.add_argument("--output", type=Path, required=True)
    maven = subparsers.add_parser("archive-maven")
    maven.add_argument("--source", type=Path, required=True)
    maven.add_argument("--output", type=Path, required=True)
    maven.add_argument("--version", required=True)
    prepare_maven = subparsers.add_parser("prepare-maven")
    prepare_maven.add_argument("--repository", type=Path, required=True)
    prepare_maven.add_argument("--version", required=True)
    metadata = subparsers.add_parser("server-metadata")
    metadata.add_argument("--output", type=Path, required=True)
    metadata.add_argument("--source-commit", required=True)
    metadata.add_argument("--binary", action="append", default=[], required=True)
    package_metadata = subparsers.add_parser("package-metadata")
    package_metadata.add_argument("--output", type=Path, required=True)
    package_metadata.add_argument("--source-commit", required=True)
    adapter_layout = subparsers.add_parser("adapter-layout")
    adapter_layout.add_argument("--release-dir", type=Path, required=True)
    adapter_layout.add_argument("--version", required=True)
    adapter_layout.add_argument("--inventory", type=Path, required=True)
    adapter_layout.add_argument("--support-matrix", type=Path, required=True)
    adapter_layout.add_argument("--output", type=Path, required=True)
    payload_hashes = subparsers.add_parser("print-payload-hashes")
    payload_hashes.add_argument("--release-dir", type=Path, required=True)
    payload_hashes.add_argument("--version", required=True)
    payload_hashes.add_argument("--inventory", type=Path, required=True)
    payload_hashes.add_argument("--support-matrix", type=Path, required=True)
    payload_hashes.add_argument("--role", action="append", default=[], required=True)
    for command in ("stage", "verify", "run-verified"):
        subparser = subparsers.add_parser(command)
        subparser.add_argument("--release-dir", type=Path, required=True)
        subparser.add_argument("--version", required=True)
        subparser.add_argument("--inventory", type=Path, required=True)
        subparser.add_argument("--support-matrix", type=Path, required=True)
        subparser.add_argument("--source-commit")
        if command == "stage":
            subparser.add_argument("--server-dir", type=Path, required=True)
            subparser.add_argument("--packages-dir", type=Path, required=True)
            subparser.add_argument("--sbom", type=Path, required=True)
            subparser.add_argument("--support-resolution", type=Path, required=True)
            subparser.add_argument("--repo-root", type=Path, required=True)
            subparser.add_argument("--source-tree", action="append", default=[], required=True)
            subparser.add_argument("--candidate-ci-run-id", required=True)
            subparser.add_argument("--candidate-ci-run-attempt", required=True, type=int)
            subparser.add_argument("--build-run-id", required=True)
            subparser.add_argument("--build-run-attempt", required=True, type=int)
        elif command == "run-verified":
            subparser.add_argument("execution", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    try:
        if args.command == "validate-id":
            validate_candidate(args.release_dir, args.version, args.source_commit)
        elif args.command == "archive-extension":
            archive_extension(args.source, args.output)
        elif args.command == "archive-maven":
            archive_maven(args.source, args.output, args.version)
        elif args.command == "prepare-maven":
            prepare_maven_repository(args.repository, args.version)
        elif args.command == "server-metadata":
            binaries = []
            for value in args.binary:
                relative, separator, path = value.partition("=")
                if not separator:
                    raise ReleaseError("binary must use staging-path=filesystem-path")
                binaries.append((relative, Path(path)))
            write_server_metadata(args.output, args.source_commit, binaries)
        elif args.command == "package-metadata":
            write_package_metadata(args.output, args.source_commit)
        elif args.command == "adapter-layout":
            materialize_adapter_layout(args.release_dir, args.version, args.inventory, args.support_matrix, args.output)
        elif args.command == "print-payload-hashes":
            print_payload_hashes(args.release_dir, args.version, args.inventory, args.support_matrix, args.role)
        elif args.command == "stage":
            if args.source_commit is None:
                parser.error("stage requires --source-commit")
            stage_release(
                release_dir=args.release_dir,
                version=args.version,
                source_commit=args.source_commit,
                inventory_path=args.inventory,
                support_matrix=args.support_matrix,
                support_resolution=args.support_resolution,
                server_dir=args.server_dir,
                packages_dir=args.packages_dir,
                sbom=args.sbom,
                repo_root=args.repo_root,
                source_trees=parse_source_trees(args.source_tree),
                candidate_ci_run_id=args.candidate_ci_run_id,
                candidate_ci_run_attempt=args.candidate_ci_run_attempt,
                build_run_id=args.build_run_id,
                build_run_attempt=args.build_run_attempt,
            )
        elif args.command == "verify":
            verify_release(args.release_dir, args.version, args.inventory, args.support_matrix, args.source_commit)
        else:
            execution = args.execution[1:] if args.execution[:1] == ["--"] else args.execution
            if args.source_commit is None:
                parser.error("run-verified requires --source-commit")
            return run_verified(args.release_dir, args.version, args.inventory, args.support_matrix, args.source_commit, execution)
    except ReleaseError as error:
        parser.error(str(error))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
