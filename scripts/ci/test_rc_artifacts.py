from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).parents[1] / "rc-artifacts.py"
SPEC = importlib.util.spec_from_file_location("rc_artifacts", SCRIPT)
assert SPEC and SPEC.loader
rc_artifacts = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(rc_artifacts)

CANDIDATE_ID = "RC-0.3.0-20260909T120000Z-d344dbd"
COMMIT = "d344dbd000000000000000000000000000000000"


class RCArtifactsTests(unittest.TestCase):
    def make_candidate(self, root: Path) -> Path:
        candidate = root / CANDIDATE_ID
        for relative in rc_artifacts.REQUIRED_ARTIFACTS:
            path = candidate / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes((relative + "\n").encode())
        archive = candidate / "artifacts/synchro-pg-pg18-linux-x64.tar.gz"
        archive.unlink()
        rc_artifacts.archive_extension(candidate / "artifacts/extension", archive)
        return candidate

    def test_seal_and_verify_reject_mutation(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            candidate = self.make_candidate(Path(directory))
            rc_artifacts.seal(candidate, CANDIDATE_ID, COMMIT)
            rc_artifacts.verify(candidate, CANDIDATE_ID, COMMIT)
            (candidate / rc_artifacts.REQUIRED_ARTIFACTS[0]).write_bytes(b"corrupt\n")
            with self.assertRaisesRegex(rc_artifacts.RCError, "artifact hash mismatch"):
                rc_artifacts.verify(candidate, CANDIDATE_ID)

    def test_verify_rejects_added_artifact(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            candidate = self.make_candidate(Path(directory))
            rc_artifacts.seal(candidate, CANDIDATE_ID, COMMIT)
            (candidate / "artifacts/extra").write_text("extra\n", encoding="utf-8")
            with self.assertRaisesRegex(rc_artifacts.RCError, "artifact set changed"):
                rc_artifacts.verify(candidate, CANDIDATE_ID)

    def test_manifest_requires_terminal_results(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            candidate = self.make_candidate(Path(directory))
            rc_artifacts.seal(candidate, CANDIDATE_ID, COMMIT)
            digest = rc_artifacts.verify(candidate, CANDIDATE_ID, COMMIT)[0]["sha256"]
            cells = candidate / "evidence/cells"
            cells.mkdir(parents=True)
            cell_path = cells / "SUP-PG-LINUX-X64-001.json"
            cell_path.write_text(json.dumps({"cell_id": "SUP-PG-LINUX-X64-001", "source_commit": COMMIT, "status": "passed", "artifact_hashes": [digest]}), encoding="utf-8")
            summary = candidate / "evidence/packaged-smoke-summary.json"
            summary.write_text(json.dumps({"source_commit": COMMIT, "status": "passed", "artifact_hashes": [digest], "obligations": [{"id": "smoke/SUP-PG-LINUX-X64-001/connect", "status": "passed", "terminal": True, "test_count": 1}]}), encoding="utf-8")
            receipt = candidate / "evidence/rc-check-pg18.json"
            receipt.write_text(json.dumps({"gate": "rc-check-pg18", "source_commit": COMMIT, "status": "passed", "terminal": True, "test_count": 1}), encoding="utf-8")
            rc_artifacts.write_manifest(candidate, CANDIDATE_ID, COMMIT, cells, summary, receipt)
            manifest = json.loads((candidate / "rc-manifest.json").read_text(encoding="utf-8"))
            self.assertEqual(manifest["candidate_id"], CANDIDATE_ID)
            self.assertEqual(manifest["executed_cells"][0]["support_cell_id"], "SUP-PG-LINUX-X64-001")

            (candidate / "rc-manifest.json").unlink()
            value = json.loads(summary.read_text(encoding="utf-8"))
            value["obligations"][0]["test_count"] = 0
            summary.write_text(json.dumps(value), encoding="utf-8")
            with self.assertRaisesRegex(rc_artifacts.RCError, "unexecuted or skipped"):
                rc_artifacts.write_manifest(candidate, CANDIDATE_ID, COMMIT, cells, summary, receipt)

    def test_verified_execution_rejects_concurrent_mutation(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            candidate = self.make_candidate(Path(directory))
            rc_artifacts.seal(candidate, CANDIDATE_ID, COMMIT)
            artifact = candidate / rc_artifacts.REQUIRED_ARTIFACTS[0]
            command = [sys.executable, "-c", "from pathlib import Path; Path(r'%s').write_bytes(b'changed')" % artifact]
            with self.assertRaisesRegex(rc_artifacts.RCError, "hash mismatch"):
                rc_artifacts.run_verified(candidate, CANDIDATE_ID, COMMIT, command)


if __name__ == "__main__":
    unittest.main()
