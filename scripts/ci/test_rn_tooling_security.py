import json
from pathlib import Path
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]


class ReactNativeToolingSecurityTests(unittest.TestCase):
    def run_probe(self, mode):
        with tempfile.TemporaryDirectory(prefix="synchro-tooling-proof-") as work:
            result = subprocess.run(
                ["node", str(ROOT / "scripts/ci/rn_tooling_security_probe.cjs"), mode, str(ROOT), work],
                capture_output=True,
                text=True,
                timeout=90,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            report = json.loads(result.stdout)
            self.assertGreater(report["cases"], 0)

    def test_cli_treats_codegen_directories_as_literal_paths(self):
        self.run_probe("cli")

    def test_assembler_preserves_json_properties_and_trace_threads(self):
        self.run_probe("assembler")

    def test_delivered_helper_applies_and_rejects_unverified_targets(self):
        self.run_probe("delivery")

    def test_cleanup_and_bob_use_literal_filesystem_directories(self):
        self.run_probe("directories")
