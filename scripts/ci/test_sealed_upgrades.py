from __future__ import annotations

import argparse
import gzip
import importlib.util
import io
import json
from pathlib import Path
import subprocess
import tarfile
import tempfile
import unittest
from unittest import mock
import zipfile


SCRIPT = Path(__file__).resolve().parents[2] / "verification/consumers/upgrade/sealed_result.py"
SPEC = importlib.util.spec_from_file_location("sealed_result", SCRIPT)
assert SPEC and SPEC.loader
sealed_result = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(sealed_result)

VERSION = "1.2.3"
SOURCE = "a" * 40


class SealedUpgradeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.release = self.root / "release"
        self.consumer = self.root / "consumer"
        self.evidence = self.root / "evidence"
        self.release.mkdir()
        self.consumer.mkdir()
        self.archive = self.swift_archive()
        self.git = mock.patch.object(
            sealed_result.subprocess, "run",
            return_value=subprocess.CompletedProcess(["git", "archive"], 0, self.archive, b""),
        )
        self.git.start()
        self.addCleanup(self.git.stop)
        apple = self.consumer / "apple"
        (apple / "Synchro").mkdir(parents=True)
        (apple / "Synchro/Package.swift").write_bytes(b"sealed Swift source\n")
        with (apple / f"synchro-spm-{VERSION}.tar.gz").open("wb") as output:
            with gzip.GzipFile(filename="", mode="wb", fileobj=output, mtime=0) as compressed:
                compressed.write(self.archive)
        maven_path = self.release / "maven.zip"
        aar_name = f"fit/trainstar/synchro/{VERSION}/synchro-{VERSION}.aar"
        with zipfile.ZipFile(maven_path, "w") as package:
            package.writestr(aar_name, b"sealed Android library")
            package.writestr(f"fit/trainstar/synchro/{VERSION}/synchro-{VERSION}.pom", b"sealed metadata")
        with zipfile.ZipFile(maven_path) as package:
            for name in package.namelist():
                target = self.consumer / "maven" / name
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(package.read(name))
        npm_path = self.release / "react-native.tgz"
        npm_path.write_bytes(b"sealed React Native package")
        (self.consumer / "npm").mkdir()
        (self.consumer / "npm/react-native.tgz").write_bytes(npm_path.read_bytes())
        self.manifest = {
            "release_version": VERSION,
            "source": {"commit": SOURCE},
            "distributions": [
                {"role": "kotlin-maven", "path": "maven.zip", "sha256": sealed_result.digest(maven_path)},
                {"role": "react-native-npm", "path": "react-native.tgz", "sha256": sealed_result.digest(npm_path)},
            ],
        }
        (self.release / "release-manifest.json").write_text(json.dumps(self.manifest))
        self.write_valid_records()

    @staticmethod
    def swift_archive() -> bytes:
        output = io.BytesIO()
        with tarfile.open(fileobj=output, mode="w") as archive:
            data = b"sealed Swift source\n"
            item = tarfile.TarInfo("Synchro/Package.swift")
            item.size = len(data)
            archive.addfile(item, io.BytesIO(data))
        return output.getvalue()

    def write_valid_records(self) -> None:
        package_hashes = sealed_result.expected_candidate_hashes(self.release, self.manifest)
        manifest_hash = sealed_result.digest(self.release / "release-manifest.json")
        directory = self.evidence / "upgrades"
        directory.mkdir(parents=True)
        for cell in sealed_result.CELLS:
            record = {
                "schema_version": 1,
                "cell_id": cell,
                "source": SOURCE,
                "sealed_manifest_sha256": manifest_hash,
                "candidate_package_hashes": package_hashes[cell],
                "predecessor_version": "0.3.1",
                "predecessor_swift_revision": sealed_result.PREDECESSOR_SWIFT_REVISION if cell == "SUP-IOS-CURRENT-001" else None,
                "tested_package_hashes": {"predecessor": "b" * 64, "candidate": "c" * 64},
                "status": "passed",
                "test": sealed_result.TEST,
                "test_count": 1,
            }
            (directory / f"{cell}.json").write_text(json.dumps(record))

    def test_valid_four_cell_result_passes(self) -> None:
        sealed_result.verify_all(self.release, self.evidence)
        cell = "SUP-IOS-CURRENT-001"
        record = json.loads((self.evidence / "upgrades" / f"{cell}.json").read_text())
        sealed_result.verify_record(record, cell, self.release / "release-manifest.json")

    def test_tampered_extracted_inputs_fail_before_upgrade(self) -> None:
        cases = (
            ("SUP-IOS-CURRENT-001", "apple/Synchro/Package.swift"),
            ("SUP-ANDROID-CURRENT-001", f"maven/fit/trainstar/synchro/{VERSION}/synchro-{VERSION}.aar"),
            ("SUP-RN-IOS-CURRENT-001", "npm/react-native.tgz"),
        )
        for cell, relative in cases:
            with self.subTest(cell=cell):
                target = self.consumer / relative
                original = target.read_bytes()
                target.write_bytes(b"tampered")
                args = argparse.Namespace(
                    release_dir=str(self.release), consumer_dir=str(self.consumer),
                    version=VERSION, cell=cell, predecessor_version="0.3.1",
                    predecessor_swift_revision=sealed_result.PREDECESSOR_SWIFT_REVISION,
                )
                with self.assertRaises(ValueError):
                    sealed_result.sealed(args)
                target.write_bytes(original)

    def test_tampered_sealed_package_fails_verification(self) -> None:
        (self.release / "react-native.tgz").write_bytes(b"different package")
        with self.assertRaisesRegex(ValueError, "React Native archive hash"):
            sealed_result.verify_all(self.release, self.evidence)

    def test_missing_extra_or_wrong_upgrade_evidence_fails(self) -> None:
        directory = self.evidence / "upgrades"
        path = directory / "SUP-IOS-CURRENT-001.json"
        original = path.read_text()
        for field, value in (
            ("source", "e" * 40),
            ("sealed_manifest_sha256", "f" * 64),
            ("status", "failed"),
            ("test_count", 0),
            ("predecessor_version", "9.9.9"),
            ("predecessor_swift_revision", "e" * 40),
            ("candidate_package_hashes", {"swift-source-tree": "0" * 64}),
            ("tested_package_hashes", {"predecessor": "b" * 64, "candidate": "wrong"}),
        ):
            with self.subTest(field=field):
                record = json.loads(original)
                record[field] = value
                path.write_text(json.dumps(record))
                with self.assertRaises(ValueError):
                    sealed_result.verify_all(self.release, self.evidence)
        path.write_text(original)
        path.unlink()
        with self.assertRaisesRegex(ValueError, "missing or extra"):
            sealed_result.verify_all(self.release, self.evidence)
        path.write_text(original)
        extra = directory / "SUP-IOS-MIN-001.json"
        extra.write_text(original)
        with self.assertRaisesRegex(ValueError, "missing or extra"):
            sealed_result.verify_all(self.release, self.evidence)
        extra.unlink()
        path.write_text(original[:-1] + ', "source": "e"}')
        with self.assertRaisesRegex(ValueError, "duplicate JSON member"):
            sealed_result.verify_all(self.release, self.evidence)
