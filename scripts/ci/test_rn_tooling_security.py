import json
import os
from pathlib import Path
import subprocess
import tarfile
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]


def run_probe(test, mode, package=None):
    with tempfile.TemporaryDirectory(prefix="synchro-tooling-proof-") as work:
        arguments = []
        if mode == "delivery" and package:
            assets = Path(work) / "package-assets"
            assets.mkdir()
            with tarfile.open(package, "r:gz") as archive:
                manifest = json.load(archive.extractfile("package/package.json"))
                test.assertEqual(manifest["name"], "@trainstar/synchro-react-native")
                test.assertEqual(manifest["version"], (ROOT / "VERSION").read_text().strip())
                for source in (ROOT / "clients/react-native/tooling-patches").iterdir():
                    shipped = archive.extractfile("package/tooling-patches/" + source.name).read()
                    test.assertEqual(shipped, source.read_bytes(), source.name)
                    key = "./tooling-patches/apply" if source.name == "apply.cjs" else "./tooling-patches/" + source.name
                    test.assertEqual(manifest["exports"][key], "./tooling-patches/" + source.name)
                    (assets / source.name).write_bytes(shipped)
            arguments.append(str(assets))
        result = subprocess.run(
            ["node", str(ROOT / "scripts/ci/rn_tooling_security_probe.cjs"), mode, str(ROOT), work, *arguments],
            capture_output=True,
            text=True,
            timeout=90,
        )
        test.assertEqual(result.returncode, 0, result.stderr)
        report = json.loads(result.stdout)
        test.assertGreater(report["cases"], 0)


class ReactNativeToolingSecurityTests(unittest.TestCase):
    def test_cli_treats_codegen_directories_as_literal_paths(self):
        run_probe(self, "cli")

    def test_assembler_preserves_json_properties_and_trace_threads(self):
        run_probe(self, "assembler")

    def test_delivered_helper_applies_and_rejects_unverified_targets(self):
        run_probe(self, "delivery")

    def test_cleanup_and_bob_use_literal_filesystem_directories(self):
        run_probe(self, "directories")


class PackagedReactNativeToolingTests(unittest.TestCase):
    def test_package_delivers_verified_tooling_corrections(self):
        package = os.environ.get("SYNCHRO_TOOLING_PACKAGE")
        self.assertTrue(package, "SYNCHRO_TOOLING_PACKAGE is required")
        run_probe(self, "delivery", package)
