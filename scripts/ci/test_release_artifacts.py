from __future__ import annotations

import gzip
import base64
import hashlib
import importlib.util
import json
import os
import subprocess
import tarfile
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = Path(__file__).parents[1] / "release-artifacts.py"
SPEC = importlib.util.spec_from_file_location("release_artifacts", SCRIPT)
assert SPEC and SPEC.loader
release_artifacts = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(release_artifacts)

VERSION = "1.2.3"
COMMIT = "a" * 40
ROOT_TREE = "b" * 40
GO_TREE = "c" * 40


def elf_x64() -> bytes:
    value = bytearray(64)
    value[:6] = b"\x7fELF\x02\x01"
    value[16:18] = (2).to_bytes(2, "little")
    value[18:20] = (62).to_bytes(2, "little")
    return bytes(value)


class ReleaseArtifactsTests(unittest.TestCase):
    def test_make_build_targets_preserve_absolute_output_paths(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for target, variable, value, output in (
                ("build", "BINARY", str(root / "synchrod-pg"), root / "synchrod-pg"),
                ("build-seed", "SEED_BINARY", str(root / "synchro-seed"), root / "synchro-seed"),
                (
                    "build-local-postgres",
                    "LOCAL_POSTGRES_BINARY",
                    "dist/release-provisioner/synchro-local-postgres",
                    REPO_ROOT / "dist/release-provisioner/synchro-local-postgres",
                ),
            ):
                result = subprocess.run(
                    ["make", "--dry-run", target, f"{variable}={value}"],
                    cwd=REPO_ROOT,
                    check=True,
                    capture_output=True,
                    text=True,
                )
                self.assertIn(f'-o "{output}"', result.stdout)
                self.assertNotIn(f"../../{output}", result.stdout)

    def write_inventory(self, root: Path) -> Path:
        artifacts = [
            self.file_artifact("PG", "pg-extension", "server", "extension/extension-{version}.tar.gz", "artifacts/extension-{version}.tar.gz"),
            self.file_artifact("ADAPTER", "adapter", "server", "adapter/adapter-{version}", "artifacts/adapter-{version}"),
            self.file_artifact("SEED", "seed-tool", "server", "seed/seed-{version}", "artifacts/seed-{version}"),
            {
                "id": "ARTDEF-GO-001", "role": "go-module", "name": "Go", "visibility": "public", "kind": "source",
                "source_path": "api/go", "source_tag_template": "api/go/v{version}", "destination_template": "go:{version}",
            },
            {
                "id": "ARTDEF-SWIFT-001", "role": "swift-spm", "name": "Swift", "visibility": "public", "kind": "source",
                "source_path": "repo-root", "source_tag_template": "v{version}", "destination_template": "swift:{version}",
            },
            {
                "id": "ARTDEF-POD-001", "role": "cocoapods", "name": "Pod", "visibility": "public", "kind": "source",
                "source_path": "repo-root", "source_tag_template": "v{version}", "destination_template": "pod:{version}",
            },
            self.file_artifact("MAVEN", "kotlin-maven", "packages", "maven/maven-{version}.zip", "artifacts/maven-{version}.zip"),
            self.file_artifact("NPM", "react-native-npm", "packages", "npm/npm-{version}.tgz", "artifacts/npm-{version}.tgz"),
            {"id": "ARTDEF-INTERNAL-001", "role": "portable-seed", "name": "Internal", "visibility": "internal", "kind": "verification"},
        ]
        path = root / "inventory.json"
        path.write_text(json.dumps({"schema_version": 1, "release": VERSION, "artifacts": artifacts}), encoding="utf-8")
        return path

    def file_artifact(self, suffix: str, role: str, stage_root: str, staging: str, released: str) -> dict[str, str]:
        return {
            "id": f"ARTDEF-{suffix}-001", "role": role, "name": suffix, "visibility": "public", "kind": "file",
            "stage_root": stage_root, "staging_path_template": staging, "release_path_template": released,
            "destination_template": f"destination-{suffix.lower()}:{{version}}",
        }

    def make_extension(self, source: Path, output: Path, unsafe: bool = False) -> None:
        library = source / "lib/synchro_pg.so"
        control = source / "share/extension/synchro_pg.control"
        sql = source / f"share/extension/synchro_pg--{VERSION}.sql"
        for path, data in ((library, elf_x64()), (control, f"default_version = '{VERSION}'\n".encode()), (sql, b"-- install\n")):
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)
        records = [
            {"path": "lib/synchro_pg.so", "destination": "pkglibdir/synchro_pg.so", "sha256": release_artifacts.file_sha256(library)},
            {"path": "share/extension/synchro_pg.control", "destination": "sharedir/extension/synchro_pg.control", "sha256": release_artifacts.file_sha256(control)},
            {"path": f"share/extension/synchro_pg--{VERSION}.sql", "destination": f"sharedir/extension/synchro_pg--{VERSION}.sql", "sha256": release_artifacts.file_sha256(sql)},
        ]
        manifest = source / "artifact-manifest.json"
        manifest.write_text(json.dumps({"format": "synchro-pg18-extension-bundle-v1", "postgresql_major": 18, "postgresql_version": "18.3", "files": records}), encoding="utf-8")
        (source / "artifact-manifest.json.sha256").write_text(release_artifacts.file_sha256(manifest) + "\n", encoding="ascii")
        release_artifacts.archive_extension(source, output)
        if unsafe:
            with output.open("wb") as output_stream:
                with gzip.GzipFile(filename="", mode="wb", fileobj=output_stream, mtime=0) as compressed:
                    with tarfile.open(fileobj=compressed, mode="w") as archive:
                        info = tarfile.TarInfo("../escape")
                        info.size = 1
                        import io
                        archive.addfile(info, io.BytesIO(b"x"))

    def make_maven(self, source: Path, output: Path, pom_version: str = VERSION, missing_signature: str = "") -> None:
        base = source / f"fit/trainstar/synchro/{VERSION}/synchro-{VERSION}"
        for suffix in (".pom", ".aar", "-sources.jar", "-javadoc.jar"):
            payload = Path(str(base) + suffix)
            payload.parent.mkdir(parents=True, exist_ok=True)
            data = suffix + "\n"
            if suffix == ".pom":
                data = f"<project><groupId>fit.trainstar</groupId><artifactId>synchro</artifactId><version>{pom_version}</version></project>\n"
            payload.write_bytes(data.encode())
            if suffix != missing_signature:
                packet = b"\xc2\x08fixture!"
                armored = "\n".join([
                    "-----BEGIN PGP SIGNATURE-----", "", base64.b64encode(packet).decode("ascii"),
                    "=" + base64.b64encode(release_artifacts.crc24(packet)).decode("ascii"),
                    "-----END PGP SIGNATURE-----", "",
                ])
                Path(str(payload) + ".asc").write_text(armored, encoding="ascii")
        release_artifacts.prepare_maven_repository(source, VERSION)
        release_artifacts.archive_maven(source, output, VERSION)

    def make_npm(self, output: Path, version: str = VERSION) -> None:
        output.parent.mkdir(parents=True, exist_ok=True)
        with output.open("wb") as output_stream:
            with gzip.GzipFile(filename="", mode="wb", fileobj=output_stream, mtime=0) as compressed:
                with tarfile.open(fileobj=compressed, mode="w") as archive:
                    data = json.dumps({"name": "@trainstar/synchro-react-native", "version": version}).encode()
                    info = tarfile.TarInfo("package/package.json")
                    info.size = len(data)
                    import io
                    archive.addfile(info, io.BytesIO(data))

    def make_fixture(self, root: Path, *, unsafe_extension: bool = False, npm_version: str = VERSION) -> dict[str, object]:
        inventory = self.write_inventory(root)
        matrix = root / "support-matrix.json"
        matrix.write_text(json.dumps({"cells": [{"id": "SUP-PG-001", "policy": "required"}, {"id": "SUP-OLD-001", "policy": "excluded"}]}), encoding="utf-8")
        support = root / "support.json"
        support.write_text(json.dumps([{"id": "SUP-PG-001", "environment": {"postgresql": "18.3", "ubuntu": "24.04", "architecture": "amd64"}}]), encoding="utf-8")
        repo = root / "repo"
        for relative in release_artifacts.DEPENDENCY_INPUTS:
            path = repo / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(relative + "\n", encoding="utf-8")
        server = root / "server"
        extension = server / f"extension/extension-{VERSION}.tar.gz"
        extension.parent.mkdir(parents=True)
        self.make_extension(root / "extension-source", extension, unsafe_extension)
        executable_paths = []
        for relative in (f"adapter/adapter-{VERSION}", f"seed/seed-{VERSION}"):
            path = server / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(elf_x64())
            path.chmod(0o755)
            executable_paths.append(relative)
        metadata = {
            "schema_version": 1,
            "source_commit": COMMIT,
            "binaries": [
                {"staging_path": relative, "format": "ELF64", "machine": "x86-64", "dependencies": {"tool": "ldd", "exit_code": 0, "output": ["fixture.so"]}}
                for relative in executable_paths
            ],
        }
        (server / release_artifacts.SERVER_METADATA_NAME).write_text(json.dumps(metadata), encoding="utf-8")
        packages = root / "packages"
        maven = packages / f"maven/maven-{VERSION}.zip"
        maven.parent.mkdir(parents=True)
        self.make_maven(root / "maven-source", maven)
        npm = packages / f"npm/npm-{VERSION}.tgz"
        self.make_npm(npm, npm_version)
        (packages / release_artifacts.PACKAGE_METADATA_NAME).write_text(json.dumps({"schema_version": 1, "source_commit": COMMIT}), encoding="utf-8")
        payloads = [extension, server / executable_paths[0], server / executable_paths[1], maven, npm]
        sbom = root / "external.spdx.json"
        sbom.write_text(json.dumps({
            "spdxVersion": "SPDX-2.3", "dataLicense": "CC0-1.0", "SPDXID": "SPDXRef-DOCUMENT",
            "name": "fixture", "documentNamespace": "https://example.invalid/sbom/fixture",
            "creationInfo": {"created": "2026-09-14T00:00:00Z", "creators": ["Tool: fixture"]},
            "files": [{"fileName": path.name, "checksums": [{"algorithm": "SHA256", "checksumValue": release_artifacts.file_sha256(path)}]} for path in payloads],
        }), encoding="utf-8")
        release_dir = root / f"release-{VERSION}-{COMMIT}"
        return {
            "release_dir": release_dir, "version": VERSION, "source_commit": COMMIT, "inventory_path": inventory,
            "support_matrix": matrix, "support_resolution": support, "server_dir": server, "packages_dir": packages,
            "sbom": sbom, "repo_root": repo, "source_trees": {"repo-root": ROOT_TREE, "api/go": GO_TREE},
            "candidate_ci_run_id": "12345", "candidate_ci_run_attempt": 2,
            "build_run_id": "67890", "build_run_attempt": 3,
        }

    def seal(self, root: Path, **fixture_options: object) -> tuple[Path, dict[str, object]]:
        arguments = self.make_fixture(root, **fixture_options)
        release_artifacts.stage_release(**arguments)
        return arguments["release_dir"], arguments

    def rewrite_sums(self, release_dir: Path) -> None:
        files = sorted(path.relative_to(release_dir).as_posix() for path in release_dir.rglob("*") if path.is_file() and path.name != release_artifacts.CHECKSUMS_NAME)
        (release_dir / release_artifacts.CHECKSUMS_NAME).write_text("".join(f"{release_artifacts.file_sha256(release_dir / item)}  {item}\n" for item in files), encoding="utf-8")

    def test_stage_and_verify_exact_distribution(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            release_dir, arguments = self.seal(Path(directory))
            records = release_artifacts.verify_release(release_dir, VERSION, arguments["inventory_path"], arguments["support_matrix"], COMMIT)
            self.assertEqual(len(records), 5)
            files = sorted(path.relative_to(release_dir).as_posix() for path in release_dir.rglob("*") if path.is_file())
            self.assertEqual(files, [
                "SHA256SUMS", f"artifacts/adapter-{VERSION}", f"artifacts/extension-{VERSION}.tar.gz",
                f"artifacts/maven-{VERSION}.zip", f"artifacts/npm-{VERSION}.tgz", f"artifacts/seed-{VERSION}",
                "release-manifest.json", "sbom.spdx.json",
            ])
            manifest = json.loads((release_dir / release_artifacts.MANIFEST_NAME).read_text(encoding="utf-8"))
            self.assertNotIn("sha256", manifest)
            self.assertEqual(manifest["source"]["source_tags"], [f"api/go/v{VERSION}", f"v{VERSION}"])
            self.assertEqual(manifest["candidate_ci"]["workflow_path"], ".github/workflows/ci.yml")
            self.assertEqual(manifest["candidate_ci"]["run_id"], "12345")
            self.assertEqual(manifest["build"]["workflow_path"], ".github/workflows/release.yml")
            self.assertEqual(manifest["build"]["run_id"], "67890")
            self.assertEqual(manifest["provenance"]["workflow_path"], ".github/workflows/release.yml")
            dependencies = {record["path"] for record in manifest["dependency_inputs"]}
            self.assertNotIn("Package.resolved", dependencies)
            self.assertIn("clients/kotlin/settings.gradle.kts", dependencies)
            self.assertIn("clients/kotlin/gradle/wrapper/gradle-wrapper.properties", dependencies)
            sums = (release_dir / release_artifacts.CHECKSUMS_NAME).read_text(encoding="utf-8")
            self.assertIn("  release-manifest.json\n", sums)
            self.assertNotIn("  SHA256SUMS\n", sums)

    def test_verify_rejects_one_byte_mutation(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            release_dir, arguments = self.seal(Path(directory))
            path = release_dir / f"artifacts/adapter-{VERSION}"
            path.write_bytes(path.read_bytes() + b"x")
            with self.assertRaisesRegex(release_artifacts.ReleaseError, "payload identity|hash mismatch"):
                release_artifacts.verify_release(release_dir, VERSION, arguments["inventory_path"], arguments["support_matrix"])

    def test_verify_rejects_added_file(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            release_dir, arguments = self.seal(Path(directory))
            (release_dir / "extra").write_text("extra", encoding="utf-8")
            with self.assertRaisesRegex(release_artifacts.ReleaseError, "missing, renamed, or extra"):
                release_artifacts.verify_release(release_dir, VERSION, arguments["inventory_path"], arguments["support_matrix"])

    def test_verify_rejects_missing_payload(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            release_dir, arguments = self.seal(Path(directory))
            (release_dir / f"artifacts/seed-{VERSION}").unlink()
            with self.assertRaisesRegex(release_artifacts.ReleaseError, "payload identity"):
                release_artifacts.verify_release(release_dir, VERSION, arguments["inventory_path"], arguments["support_matrix"])

    def test_stage_rejects_wrong_package_version(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            arguments = self.make_fixture(Path(directory), npm_version="9.9.9")
            with self.assertRaisesRegex(release_artifacts.ReleaseError, "name or version"):
                release_artifacts.stage_release(**arguments)

    def test_stage_rejects_release_run_as_candidate_ci(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            arguments = self.make_fixture(Path(directory))
            arguments["candidate_ci_run_id"] = arguments["build_run_id"]
            with self.assertRaisesRegex(release_artifacts.ReleaseError, "different workflow runs"):
                release_artifacts.stage_release(**arguments)

    def test_maven_archive_rejects_wrong_coordinates_version(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with self.assertRaisesRegex(release_artifacts.ReleaseError, "coordinates or version"):
                self.make_maven(root / "repository", root / "bundle.zip", pom_version="9.9.9")

    def test_maven_preparation_rejects_missing_signature(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with self.assertRaisesRegex(release_artifacts.ReleaseError, "lacks payload or detached signature"):
                self.make_maven(root / "repository", root / "bundle.zip", missing_signature=".aar")

    def test_maven_preparation_generates_exact_lowercase_checksums(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            repository = root / "repository"
            self.make_maven(repository, root / "bundle.zip")
            base = repository / f"fit/trainstar/synchro/{VERSION}/synchro-{VERSION}.aar"
            md5 = Path(str(base) + ".md5")
            sha1 = Path(str(base) + ".sha1")
            self.assertEqual(md5.read_text(encoding="ascii").strip(), hashlib.md5(base.read_bytes()).hexdigest())
            self.assertEqual(sha1.read_text(encoding="ascii").strip(), hashlib.sha1(base.read_bytes()).hexdigest())
            signature = Path(str(base) + ".asc")
            self.assertEqual(Path(str(signature) + ".md5").read_text(encoding="ascii").strip(), hashlib.md5(signature.read_bytes()).hexdigest())
            md5.write_text("A" * 32 + "\n", encoding="ascii")
            with self.assertRaisesRegex(release_artifacts.ReleaseError, "checksum is invalid"):
                release_artifacts.archive_maven(repository, root / "mutated.zip", VERSION)

    def test_verify_rejects_wrong_source_tag(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            release_dir, arguments = self.seal(Path(directory))
            manifest_path = release_dir / release_artifacts.MANIFEST_NAME
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest["distributions"][3]["source_tag"] = "api/go/v9.9.9"
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
            self.rewrite_sums(release_dir)
            with self.assertRaisesRegex(release_artifacts.ReleaseError, "source record"):
                release_artifacts.verify_release(release_dir, VERSION, arguments["inventory_path"], arguments["support_matrix"])

    def test_stage_rejects_unsafe_extension_archive(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            arguments = self.make_fixture(Path(directory), unsafe_extension=True)
            with self.assertRaisesRegex(release_artifacts.ReleaseError, "unsafe path"):
                release_artifacts.stage_release(**arguments)

    def test_stage_rejects_incomplete_canonical_set(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            arguments = self.make_fixture(Path(directory))
            (arguments["packages_dir"] / f"maven/maven-{VERSION}.zip").unlink()
            with self.assertRaisesRegex(release_artifacts.ReleaseError, "incomplete or unexpected"):
                release_artifacts.stage_release(**arguments)

    def test_stage_rejects_symlinked_payload(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            arguments = self.make_fixture(Path(directory))
            path = arguments["server_dir"] / f"seed/seed-{VERSION}"
            path.unlink()
            path.symlink_to(arguments["server_dir"] / f"adapter/adapter-{VERSION}")
            with self.assertRaisesRegex(release_artifacts.ReleaseError, "unsupported entry"):
                release_artifacts.stage_release(**arguments)

    def test_stage_rejects_package_source_substitution(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            arguments = self.make_fixture(Path(directory))
            metadata = arguments["packages_dir"] / release_artifacts.PACKAGE_METADATA_NAME
            metadata.write_text(json.dumps({"schema_version": 1, "source_commit": "d" * 40}), encoding="utf-8")
            with self.assertRaisesRegex(release_artifacts.ReleaseError, "do not match the release source"):
                release_artifacts.stage_release(**arguments)

    def test_adapter_layout_uses_sealed_manifest_digest(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            release_dir, arguments = self.seal(Path(directory))
            output = Path(directory) / "adapter-layout"
            release_artifacts.materialize_adapter_layout(
                release_dir, VERSION, arguments["inventory_path"], arguments["support_matrix"], output,
            )
            adapter = output / "synchrod-pg"
            self.assertTrue(os.access(adapter, os.X_OK))
            self.assertEqual(
                (output / "synchrod-pg.sha256").read_text(encoding="ascii").strip(),
                release_artifacts.file_sha256(adapter),
            )

    def test_verify_rejects_checksum_self_reference(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            release_dir, arguments = self.seal(Path(directory))
            checksums = release_dir / release_artifacts.CHECKSUMS_NAME
            checksums.write_text(checksums.read_text(encoding="utf-8") + f"{'0' * 64}  SHA256SUMS\n", encoding="utf-8")
            with self.assertRaisesRegex(release_artifacts.ReleaseError, "hash mismatch|circularity"):
                release_artifacts.verify_release(release_dir, VERSION, arguments["inventory_path"], arguments["support_matrix"])

    def test_candidate_identity_binds_version_and_full_commit(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with self.assertRaisesRegex(release_artifacts.ReleaseError, "directory name"):
                release_artifacts.validate_candidate(root / f"release-{VERSION}-{'a' * 7}", VERSION, COMMIT)
            with self.assertRaisesRegex(release_artifacts.ReleaseError, "release version"):
                release_artifacts.validate_candidate(root / f"release-{VERSION}-{COMMIT}", "1.2", COMMIT)


if __name__ == "__main__":
    unittest.main()
