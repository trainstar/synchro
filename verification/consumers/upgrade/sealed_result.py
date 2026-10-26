#!/usr/bin/env python3
"""Bind a native upgrade result to sealed package inputs and executed tests."""

import argparse
import gzip
import hashlib
import io
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tarfile
import zipfile


CELLS = {
    "SUP-IOS-CURRENT-001": ("swift", "swift"),
    "SUP-ANDROID-CURRENT-001": ("kotlin", "kotlin"),
    "SUP-RN-IOS-CURRENT-001": ("rn-ios", "rn-ios"),
    "SUP-RN-ANDROID-CURRENT-001": ("rn-android", "rn-android"),
}
TEST = "TestNativePackageUpgrade"
APPLE_SOURCE_PATHS = ("Package.swift", "Synchro.podspec", "LICENSE", "clients/swift/Sources")
REPO_ROOT = Path(__file__).resolve().parents[3]
PREDECESSOR_VERSION = "0.3.1"
PREDECESSOR_SWIFT_REVISION = "234d18d0f8f1751927ca58544863688ebc2fb70a"
SHA256 = re.compile(r"[0-9a-f]{64}\Z")
COMMIT = re.compile(r"[0-9a-f]{40}\Z")
RESULT_KEYS = {
    "schema_version", "package_job_cell_id", "upgrade_environment", "source", "sealed_manifest_sha256",
    "candidate_package_hashes", "predecessor_version", "predecessor_swift_revision",
    "tested_package_hashes", "status", "test", "test_count",
}
UPGRADE_ENVIRONMENTS = {
    "SUP-IOS-CURRENT-001": {"mode": "swift-host", "host_platform": "darwin"},
    "SUP-ANDROID-CURRENT-001": {"mode": "kotlin-android-device"},
    "SUP-RN-IOS-CURRENT-001": {"mode": "react-native-ios-simulator", "react_native_version": "0.83.10"},
    "SUP-RN-ANDROID-CURRENT-001": {"mode": "react-native-android-device", "react_native_version": "0.88.0"},
}


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def tree(path: Path, allow_links: bool = False) -> str:
    if not path.is_dir() or path.is_symlink():
        raise ValueError(f"package directory is missing or unsafe: {path}")
    files = sorted(p for p in path.rglob("*") if p.is_file() or p.is_symlink())
    if not files or (not allow_links and any(p.is_symlink() for p in path.rglob("*"))):
        raise ValueError(f"package directory is empty or contains links: {path}")
    result = hashlib.sha256()
    for item in files:
        result.update(item.relative_to(path).as_posix().encode() + b"\0")
        if item.is_symlink():
            result.update(b"link\0" + os.readlink(item).encode())
        else:
            result.update(bytes.fromhex(digest(item)))
    return result.hexdigest()


def sealed(args: argparse.Namespace) -> dict:
    if args.predecessor_version != PREDECESSOR_VERSION:
        raise ValueError("upgrade predecessor version differs from the published origin")
    if args.cell == "SUP-IOS-CURRENT-001" and args.predecessor_swift_revision != PREDECESSOR_SWIFT_REVISION:
        raise ValueError("Swift predecessor revision differs from the published origin")
    release = Path(args.release_dir)
    consumer = Path(args.consumer_dir)
    manifest_path = release / "release-manifest.json"
    manifest = json.loads(manifest_path.read_text())
    if manifest["release_version"] != args.version:
        raise ValueError("sealed release version differs from requested version")
    roles = {record["role"]: record for record in manifest["distributions"]}
    source = manifest["source"]["commit"]
    cell = args.cell
    hashes = {}
    if cell in {"SUP-IOS-CURRENT-001", "SUP-RN-IOS-CURRENT-001"}:
        apple = consumer / "apple"
        archive_path = apple / f"synchro-spm-{args.version}.tar.gz"
        archived_source = gzip.decompress(archive_path.read_bytes())
        committed_source = subprocess.run(
            ["git", "-C", str(REPO_ROOT), "archive", "--format=tar", "--prefix=Synchro/", source, *APPLE_SOURCE_PATHS],
            check=True, capture_output=True,
        ).stdout
        if archived_source != committed_source:
            raise ValueError("extracted Swift archive differs from the sealed source commit")
        with tarfile.open(fileobj=io.BytesIO(archived_source), mode="r:") as contents:
            archived_files = {item.name.removeprefix("Synchro/"): item for item in contents if item.isfile()}
            actual_files = {item.relative_to(apple / "Synchro").as_posix() for item in (apple / "Synchro").rglob("*") if item.is_file()}
            if archived_files.keys() != actual_files:
                raise ValueError("extracted Swift source files differ from the sealed archive")
            for name, item in archived_files.items():
                stream = contents.extractfile(item)
                if stream is None or hashlib.sha256(stream.read()).hexdigest() != digest(apple / "Synchro" / name):
                    raise ValueError(f"extracted Swift source changed: {name}")
        hashes["swift-source-tree"] = tree(apple / "Synchro")
        hashes["swift-source-archive"] = digest(archive_path)
    if cell in {"SUP-ANDROID-CURRENT-001", "SUP-RN-ANDROID-CURRENT-001"}:
        archive_record = roles["kotlin-maven"]
        archive = release / archive_record["path"]
        if digest(archive) != archive_record["sha256"]:
            raise ValueError("sealed Maven archive changed")
        extracted = consumer / "maven"
        with zipfile.ZipFile(archive) as contents:
            names = {item.filename for item in contents.infolist() if not item.is_dir()}
            if any(Path(name).is_absolute() or ".." in Path(name).parts for name in names):
                raise ValueError("sealed Maven archive has an unsafe path")
            actual = {item.relative_to(extracted).as_posix() for item in extracted.rglob("*") if item.is_file()}
            if names != actual:
                raise ValueError("extracted Maven files differ from the sealed archive")
            for name in names:
                if hashlib.sha256(contents.read(name)).hexdigest() != digest(extracted / name):
                    raise ValueError(f"extracted Maven package changed: {name}")
        hashes["kotlin-maven-archive"] = archive_record["sha256"]
        hashes["kotlin-maven-files"] = tree(extracted)
        hashes["kotlin-aar"] = digest(extracted / "fit" / "trainstar" / "synchro" / args.version / f"synchro-{args.version}.aar")
    if cell.startswith("SUP-RN-"):
        npm_record = roles["react-native-npm"]
        npm = consumer / "npm" / Path(npm_record["path"]).name
        if digest(npm) != npm_record["sha256"]:
            raise ValueError("extracted React Native package differs from the sealed archive")
        hashes["react-native-npm"] = npm_record["sha256"]
    return {
        "source": source,
        "sealed_manifest_sha256": digest(manifest_path),
        "candidate_package_hashes": hashes,
        "predecessor_version": args.predecessor_version,
        "predecessor_swift_revision": args.predecessor_swift_revision if cell == "SUP-IOS-CURRENT-001" else None,
    }


def runtime(args: argparse.Namespace) -> dict:
    kind, directory = CELLS[args.cell]
    work = Path(args.work_dir) / directory
    if kind == "swift":
        if sys.platform != "darwin":
            raise ValueError("Swift host upgrade requires macOS")
        packages = {
            side: digest(work / side / ".build" / "debug" / "SynchroUpgrade")
            for side in ("predecessor", "candidate")
        }
    elif kind == "kotlin":
        packages = {
            side: digest(work / side / "app" / "build" / "outputs" / "apk" / "debug" / "app-debug.apk")
            for side in ("predecessor", "candidate")
        }
    elif kind == "rn-android":
        packages = {side: digest(work / f"{side}.apk") for side in ("predecessor", "candidate")}
    else:
        packages = {side: tree(work / f"{side}.app", allow_links=True) for side in ("predecessor", "candidate")}
    environment = UPGRADE_ENVIRONMENTS[args.cell].copy()
    if kind == "swift":
        environment["host_platform"] = sys.platform
    if kind.startswith("rn-"):
        package = json.loads((work / "app" / "package.json").read_text(), object_pairs_hook=unique_members)
        version = package["dependencies"]["react-native"]
        if version != environment["react_native_version"]:
            raise ValueError(f"generated React Native host version is wrong: {version}")
        environment["react_native_version"] = version
    return {"package_hashes": packages, "upgrade_environment": environment}


def completed_test(path: Path) -> int:
    events = []
    accepted = False
    for line in path.read_text(errors="replace").splitlines():
        if line.strip() == "testresult: target_pass":
            accepted = True
        if not line.startswith("{"):
            continue
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if event.get("Package", "").endswith("/upgrade") and event.get("Action"):
            events.append(event)
    if not accepted or sum(e.get("Action") == "pass" and e.get("Test") == TEST for e in events) != 1:
        raise ValueError("structured native upgrade success is missing")
    if any(e.get("Action") in {"fail", "skip"} for e in events):
        raise ValueError("native upgrade contains a failed or skipped test")
    if sum(e.get("Action") == "pass" and not e.get("Test") for e in events) != 1:
        raise ValueError("native upgrade package did not pass")
    return 1


def write_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + f".tmp.{os.getpid()}")
    temporary.write_text(json.dumps(value, sort_keys=True, indent=2) + "\n")
    temporary.replace(path)


def unique_members(pairs: list[tuple[str, object]]) -> dict:
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON member: {key}")
        result[key] = value
    return result


def archive_file_hashes(data: bytes, prefix: str = "") -> dict[str, str]:
    with tarfile.open(fileobj=io.BytesIO(data), mode="r:") as archive:
        result = {}
        for item in archive:
            if not item.isfile():
                continue
            if not item.name.startswith(prefix):
                raise ValueError("Swift source archive has an unexpected path")
            relative = item.name[len(prefix):]
            if not relative or relative in result or Path(relative).is_absolute() or ".." in Path(relative).parts:
                raise ValueError("Swift source archive has an unsafe or duplicate file")
            stream = archive.extractfile(item)
            if stream is None:
                raise ValueError("Swift source archive file is unreadable")
            result[relative] = hashlib.sha256(stream.read()).hexdigest()
    if not result:
        raise ValueError("Swift source archive is empty")
    return result


def file_tree_hash(files: dict[str, str]) -> str:
    result = hashlib.sha256()
    for name, value in sorted(files.items()):
        result.update(name.encode() + b"\0")
        result.update(bytes.fromhex(value))
    return result.hexdigest()


def expected_candidate_hashes(release: Path, manifest: dict, repo_root: Path = REPO_ROOT) -> dict[str, dict[str, str]]:
    source = manifest["source"]["commit"]
    if not isinstance(source, str) or not COMMIT.fullmatch(source):
        raise ValueError("sealed release source commit is invalid")
    records = manifest["distributions"]
    roles = {record["role"]: record for record in records}
    if len(roles) != len(records):
        raise ValueError("sealed release repeats a package role")
    archive = subprocess.run(
        ["git", "-C", str(repo_root), "archive", "--format=tar", "--prefix=Synchro/", source, *APPLE_SOURCE_PATHS],
        check=True, capture_output=True,
    ).stdout
    swift_files = archive_file_hashes(archive, "Synchro/")
    apple_compressed = io.BytesIO()
    with gzip.GzipFile(filename="", mode="wb", fileobj=apple_compressed, mtime=0) as stream:
        stream.write(archive)
    apple = {
        "swift-source-tree": file_tree_hash(swift_files),
        "swift-source-archive": hashlib.sha256(apple_compressed.getvalue()).hexdigest(),
    }
    maven_record = roles["kotlin-maven"]
    maven_archive = release / maven_record["path"]
    if digest(maven_archive) != maven_record["sha256"]:
        raise ValueError("sealed Maven archive hash is invalid")
    maven_files = {}
    with zipfile.ZipFile(maven_archive) as contents:
        for item in contents.infolist():
            if item.is_dir():
                continue
            name = item.filename
            if name in maven_files or Path(name).is_absolute() or ".." in Path(name).parts:
                raise ValueError("sealed Maven archive has an unsafe or duplicate file")
            maven_files[name] = hashlib.sha256(contents.read(item)).hexdigest()
    aar = f"fit/trainstar/synchro/{manifest['release_version']}/synchro-{manifest['release_version']}.aar"
    kotlin = {
        "kotlin-maven-archive": maven_record["sha256"],
        "kotlin-maven-files": file_tree_hash(maven_files),
        "kotlin-aar": maven_files[aar],
    }
    npm_record = roles["react-native-npm"]
    if digest(release / npm_record["path"]) != npm_record["sha256"]:
        raise ValueError("sealed React Native archive hash is invalid")
    npm = {"react-native-npm": npm_record["sha256"]}
    return {
        "SUP-IOS-CURRENT-001": apple,
        "SUP-ANDROID-CURRENT-001": kotlin,
        "SUP-RN-IOS-CURRENT-001": apple | npm,
        "SUP-RN-ANDROID-CURRENT-001": kotlin | npm,
    }


def verify_record(record: dict, cell_id: str, manifest_path: Path, repo_root: Path = REPO_ROOT) -> None:
    if cell_id not in CELLS:
        raise ValueError(f"unsupported upgrade cell: {cell_id}")
    manifest_path = Path(manifest_path)
    manifest = json.loads(manifest_path.read_text(), object_pairs_hook=unique_members)
    expected = expected_candidate_hashes(manifest_path.parent, manifest, repo_root=repo_root)
    source = manifest["source"]["commit"]
    manifest_hash = digest(manifest_path)
    if not isinstance(record, dict) or set(record) != RESULT_KEYS:
        raise ValueError(f"upgrade result shape is invalid: {cell_id}")
    if record["schema_version"] != 1 or type(record["schema_version"]) is not int:
        raise ValueError(f"upgrade result schema is invalid: {cell_id}")
    if record["package_job_cell_id"] != cell_id or record["source"] != source or record["sealed_manifest_sha256"] != manifest_hash:
        raise ValueError(f"upgrade result identity is invalid: {cell_id}")
    if record["upgrade_environment"] != UPGRADE_ENVIRONMENTS[cell_id]:
        raise ValueError(f"upgrade harness environment is invalid: {cell_id}")
    if record["candidate_package_hashes"] != expected[cell_id]:
        raise ValueError(f"upgrade candidate package hashes are invalid: {cell_id}")
    built = record["tested_package_hashes"]
    if not isinstance(built, dict) or set(built) != {"predecessor", "candidate"} or any(
        not isinstance(value, str) or not SHA256.fullmatch(value) for value in built.values()
    ):
        raise ValueError(f"upgrade tested package hashes are invalid: {cell_id}")
    revision = record["predecessor_swift_revision"]
    if cell_id == "SUP-IOS-CURRENT-001":
        if revision != PREDECESSOR_SWIFT_REVISION:
            raise ValueError("Swift predecessor revision is invalid")
    elif revision is not None:
        raise ValueError(f"unexpected Swift predecessor revision: {cell_id}")
    if record["predecessor_version"] != PREDECESSOR_VERSION:
        raise ValueError(f"upgrade predecessor version is invalid: {cell_id}")
    if record["status"] != "passed" or record["test"] != TEST or type(record["test_count"]) is not int or record["test_count"] != 1:
        raise ValueError(f"upgrade test result is invalid: {cell_id}")


def verify_all(release_dir: Path, evidence_dir: Path) -> None:
    manifest_path = release_dir / "release-manifest.json"
    directory = evidence_dir / "upgrades"
    if not directory.is_dir() or directory.is_symlink():
        raise ValueError("upgrade evidence directory is missing or unsafe")
    names = {item.name for item in directory.iterdir()}
    required = {f"{cell}.json" for cell in CELLS}
    if names != required:
        raise ValueError(f"upgrade evidence has missing or extra cells: {sorted(names ^ required)}")
    for cell in CELLS:
        path = directory / f"{cell}.json"
        if not path.is_file() or path.is_symlink():
            raise ValueError(f"upgrade evidence is missing or unsafe: {cell}")
        record = json.loads(path.read_text(), object_pairs_hook=unique_members)
        verify_record(record, cell, manifest_path)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("snapshot", "runtime-snapshot", "record", "verify-all"))
    parser.add_argument("--cell", choices=CELLS)
    parser.add_argument("--work-dir")
    parser.add_argument("--output")
    parser.add_argument("--release-dir")
    parser.add_argument("--evidence-dir")
    parser.add_argument("--consumer-dir")
    parser.add_argument("--version")
    parser.add_argument("--predecessor-version")
    parser.add_argument("--predecessor-swift-revision")
    parser.add_argument("--before")
    parser.add_argument("--runtime-before")
    parser.add_argument("--test-log")
    args = parser.parse_args()
    if args.action == "verify-all":
        if not args.release_dir or not args.evidence_dir or any((
            args.cell, args.work_dir, args.output, args.consumer_dir, args.version,
            args.predecessor_version, args.predecessor_swift_revision,
            args.before, args.runtime_before, args.test_log,
        )):
            parser.error("verify-all requires only --release-dir and --evidence-dir")
        verify_all(Path(args.release_dir), Path(args.evidence_dir))
        print("All four sealed client upgrade results passed verification")
        return
    if not all((args.cell, args.work_dir, args.output)):
        parser.error("upgrade snapshot and record actions require cell, work directory, and output")
    output = Path(args.output)
    if args.action == "runtime-snapshot":
        write_json(output, runtime(args))
        return
    if not all((args.release_dir, args.consumer_dir, args.version, args.predecessor_version)):
        parser.error("sealed release and predecessor inputs are required")
    if args.cell == "SUP-IOS-CURRENT-001" and not args.predecessor_swift_revision:
        parser.error("Swift predecessor revision is required")
    current = sealed(args)
    if args.action == "snapshot":
        write_json(output, current)
        return
    if not all((args.before, args.runtime_before, args.test_log)):
        parser.error("prior snapshots and test log are required")
    before = json.loads(Path(args.before).read_text())
    if before != current:
        raise ValueError("sealed upgrade inputs changed during execution")
    runtime_before = json.loads(Path(args.runtime_before).read_text(), object_pairs_hook=unique_members)
    if runtime_before != runtime(args):
        raise ValueError("built upgrade packages changed during execution")
    count = completed_test(Path(args.test_log))
    write_json(output, {
        "schema_version": 1,
        "package_job_cell_id": args.cell,
        "upgrade_environment": runtime_before["upgrade_environment"],
        "source": current["source"],
        "sealed_manifest_sha256": current["sealed_manifest_sha256"],
        "candidate_package_hashes": current["candidate_package_hashes"],
        "predecessor_version": current["predecessor_version"],
        "predecessor_swift_revision": current["predecessor_swift_revision"],
        "tested_package_hashes": runtime_before["package_hashes"],
        "status": "passed",
        "test": TEST,
        "test_count": count,
    })


if __name__ == "__main__":
    try:
        main()
    except (OSError, KeyError, TypeError, ValueError, zipfile.BadZipFile, tarfile.TarError, subprocess.CalledProcessError) as error:
        print(f"sealed upgrade result: {error}", file=sys.stderr)
        sys.exit(1)
