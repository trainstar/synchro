from __future__ import annotations

import copy
import json
import unittest
from pathlib import Path

from verification import support_environments as environments


def valid_records() -> list[dict[str, object]]:
    """Deterministic contract fixtures, not measurements of executed environments."""
    return [
        {"id": "SUP-PG-LINUX-X64-001", "environment": {
            "architecture": "x86_64", "os": "ubuntu-24.04", "postgresql": "18.3",
        }},
        {"id": "SUP-IOS-MIN-001", "environment": {"ios": "16.4", "xcode": "16.4"}},
        {"id": "SUP-IOS-CURRENT-001", "environment": {"ios": "27.0", "xcode": "27.0"}},
        {"id": "SUP-RN-IOS-CURRENT-001", "environment": {
            "ios": "27.0", "xcode": "27.0", "react_native": "0.83.10",
        }},
        {"id": "SUP-ANDROID-MIN-001", "environment": {
            "android_api": "24", "os": "ubuntu-24.04",
            "system_image": "system-images;android-24;google_apis;x86_64",
            "system_image_revision": "27", "emulator_version": "37.2.12", "emulator_build": "16428233",
        }},
        {"id": "SUP-ANDROID-CURRENT-001", "environment": {
            "android_api": "37", "os": "ubuntu-24.04",
            "system_image": "system-images;android-37.0;google_apis;x86_64",
            "system_image_revision": "6", "emulator_version": "37.2.12", "emulator_build": "16428233",
        }},
        {"id": "SUP-RN-ANDROID-CURRENT-001", "environment": {
            "android_api": "37", "os": "ubuntu-24.04",
            "system_image": "system-images;android-37.0;google_apis;x86_64",
            "system_image_revision": "6", "emulator_version": "37.2.12", "emulator_build": "16428233",
            "react_native": "0.83.10",
        }},
    ]


def invalid_collections() -> list[tuple[str, object]]:
    cases: list[tuple[str, object]] = []
    for cell_id, field, value in (
        ("SUP-PG-LINUX-X64-001", "postgresql", "17.3"),
        ("SUP-PG-LINUX-X64-001", "postgresql", "18.03"),
        ("SUP-PG-LINUX-X64-001", "postgresql", "18.3.0"),
        ("SUP-PG-LINUX-X64-001", "architecture", "amd64"),
        ("SUP-PG-LINUX-X64-001", "os", "ubuntu-22.04"),
        ("SUP-IOS-MIN-001", "ios", "17.0"),
        ("SUP-IOS-CURRENT-001", "ios", "15.9"),
        ("SUP-IOS-CURRENT-001", "ios", "27.0.1"),
        ("SUP-IOS-CURRENT-001", "ios", "027.0"),
        ("SUP-IOS-CURRENT-001", "xcode", "27"),
        ("SUP-IOS-CURRENT-001", "xcode", "27.0.0.0"),
        ("SUP-IOS-CURRENT-001", "xcode", "27.0-beta"),
        ("SUP-RN-IOS-CURRENT-001", "react_native", "0.83.4"),
        ("SUP-RN-IOS-CURRENT-001", "react_native", "0.84.0"),
        ("SUP-RN-ANDROID-CURRENT-001", "react_native", "0.083.10"),
        ("SUP-RN-ANDROID-CURRENT-001", "react_native", "0.83"),
        ("SUP-ANDROID-MIN-001", "android_api", "25"),
        ("SUP-ANDROID-CURRENT-001", "android_api", "23"),
        ("SUP-ANDROID-CURRENT-001", "android_api", "037"),
        ("SUP-ANDROID-CURRENT-001", "android_api", "37.0"),
        ("SUP-ANDROID-CURRENT-001", "android_api", "38"),
        ("SUP-ANDROID-CURRENT-001", "system_image", "system-images;android-36;google_apis;x86_64"),
        ("SUP-ANDROID-CURRENT-001", "system_image", "system-images;android-37.1;google_apis;x86_64"),
        ("SUP-ANDROID-CURRENT-001", "system_image", "system-images;android-037.0;google_apis;x86_64"),
        ("SUP-ANDROID-CURRENT-001", "system_image", "system-images;android-37.0;google_apis;arm64-v8a"),
        ("SUP-ANDROID-CURRENT-001", "system_image", "system-images;android-37.0;google_apis;x86_64;extra"),
        ("SUP-ANDROID-CURRENT-001", "os", "macos-26"),
        ("SUP-ANDROID-CURRENT-001", "system_image_revision", "0"),
        ("SUP-ANDROID-CURRENT-001", "system_image_revision", "06"),
        ("SUP-ANDROID-CURRENT-001", "system_image_revision", "6.0"),
        ("SUP-ANDROID-CURRENT-001", "emulator_version", "37.2.12.0"),
        ("SUP-ANDROID-CURRENT-001", "emulator_version", "37.02.12"),
        ("SUP-ANDROID-CURRENT-001", "emulator_version", "37.2.12-beta"),
        ("SUP-ANDROID-CURRENT-001", "emulator_build", "016428233"),
        ("SUP-ANDROID-CURRENT-001", "emulator_build", "0"),
        ("SUP-ANDROID-CURRENT-001", "emulator_build", "build-16428233"),
    ):
        records = valid_records()
        next(record for record in records if record["id"] == cell_id)["environment"][field] = value
        cases.append((f"{cell_id}/{field}/{value}", records))
    records = valid_records()
    records.append(copy.deepcopy(records[0]))
    cases.append(("duplicate record", records))
    cases.append(("missing cell", valid_records()[:-1]))
    for cell_id in ("SUP-MACOS-CURRENT-001", "SUP-PG-014", "SUP-UNKNOWN", 1, []):
        records = valid_records()
        records[0]["id"] = cell_id
        cases.append((f"invalid ID {cell_id!r}", records))
    for change in ({"unknown": "18.3"}, {"postgresql": ""}, {"postgresql": 18},
                   {"postgresql": "current"}, {"postgresql": "18.x"}, {"postgresql": "18.*"}):
        records = valid_records()
        records[0]["environment"].update(change)
        cases.append((f"invalid environment {change!r}", records))
    records = valid_records()
    del records[0]["environment"]["postgresql"]
    cases.append(("missing field", records))
    records = valid_records()
    records[0]["unexpected"] = True
    cases.append(("unknown record field", records))
    return cases


class SupportEnvironmentTests(unittest.TestCase):
    def test_all_profiles_and_equal_values_are_valid(self) -> None:
        records = valid_records()
        for record in records:
            with self.subTest(cell=record["id"]):
                self.assertEqual(environments.validate_environment(record["id"], record["environment"]), record["environment"])
        result = environments.validate_records(list(reversed(records)))
        self.assertEqual([record["id"] for record in result], sorted(environments.REQUIRED_IDS))
        self.assertEqual(result, environments.validate_records(records))
        self.assertEqual(result, json.loads(json.dumps(result), object_pairs_hook=environments.reject_duplicate_members))

    def test_invalid_collections_fail(self) -> None:
        for label, records in invalid_collections():
            with self.subTest(case=label), self.assertRaises(environments.EnvironmentError):
                environments.validate_records(records)

    def test_each_profile_rejects_missing_unknown_empty_and_nonstring_fields(self) -> None:
        for record in valid_records():
            for field in record["environment"]:
                for bad in ("", None, 1, True, [], {}, "latest", "stable", "*"):
                    value = {**record["environment"], field: bad}
                    with self.subTest(cell=record["id"], field=field, bad=bad), self.assertRaises(environments.EnvironmentError):
                        environments.validate_environment(record["id"], value)
                value = dict(record["environment"])
                del value[field]
                with self.subTest(cell=record["id"], missing=field), self.assertRaises(environments.EnvironmentError):
                    environments.validate_environment(record["id"], value)
            with self.subTest(cell=record["id"], unknown=True), self.assertRaises(environments.EnvironmentError):
                environments.validate_environment(record["id"], {**record["environment"], "unknown": "1"})

    def test_canonical_numeric_and_image_forms(self) -> None:
        records = valid_records()
        records[0]["environment"]["postgresql"] = "18.0"
        records[1]["environment"].update(ios="16.0.0", xcode="16.4.1")
        for record in records:
            if "system_image" in record["environment"]:
                api = record["environment"]["android_api"]
                for suffix in ("", ".0"):
                    value = {**record["environment"], "system_image": f"system-images;android-{api}{suffix};google_apis;x86_64"}
                    environments.validate_environment(record["id"], value)
        environments.validate_records(records)
        for bad in ("-1", "+6", "6 ", " 6", "6\n", "٦", "6.00"):
            value = {**records[5]["environment"], "system_image_revision": bad}
            with self.subTest(bad=bad), self.assertRaises(environments.EnvironmentError):
                environments.validate_environment(records[5]["id"], value)

    def test_react_native_xcode_minimum_boundary(self) -> None:
        value = valid_records()[3]["environment"]
        for xcode, runtime, accepted in (
            ("26.3", "0.83.4", True), ("26.4", "0.83.4", False),
            ("26.4.0", "0.83.4", False), ("26.4", "0.83.5", True),
            ("27.0", "0.83.5", True), ("27.0", "0.83.10", True),
        ):
            with self.subTest(xcode=xcode, runtime=runtime):
                candidate = {**value, "xcode": xcode, "react_native": runtime}
                if accepted:
                    environments.validate_environment("SUP-RN-IOS-CURRENT-001", candidate)
                else:
                    with self.assertRaises(environments.EnvironmentError):
                        environments.validate_environment("SUP-RN-IOS-CURRENT-001", candidate)

    def test_current_consistency_without_claiming_stable_authority(self) -> None:
        records = valid_records()
        for record in records:
            if record["id"] in {"SUP-IOS-CURRENT-001", "SUP-RN-IOS-CURRENT-001"}:
                record["environment"]["ios"] = "16.0"
            if record["id"] in {"SUP-ANDROID-CURRENT-001", "SUP-RN-ANDROID-CURRENT-001"}:
                record["environment"].update(android_api="24", system_image="system-images;android-24;google_apis;x86_64")
        environments.validate_records(records)
        records[-1]["environment"].update(android_api="25", system_image="system-images;android-25;google_apis;x86_64")
        with self.assertRaisesRegex(environments.EnvironmentError, "current android_api versions differ"):
            environments.validate_records(records)

    def test_explicit_incomplete_mode_keeps_present_records_strict(self) -> None:
        self.assertEqual(environments.validate_records([], complete=False), [])
        records = valid_records()[:1]
        self.assertEqual(environments.validate_records(records, complete=False), records)
        with self.assertRaises(environments.EnvironmentError):
            environments.validate_records(records)
        for label, bad in invalid_collections():
            if label == "missing cell":
                continue
            with self.subTest(case=label), self.assertRaises(environments.EnvironmentError):
                environments.validate_records(bad, complete=False)

    def test_malformed_collections_and_records(self) -> None:
        for value in (None, {}, "", 1, [None], [{}], [{"id": "SUP-IOS-MIN-001"}]):
            with self.subTest(value=value), self.assertRaises(environments.EnvironmentError):
                environments.validate_records(value)
        for value in (None, [], "", 1):
            with self.subTest(environment=value), self.assertRaises(environments.EnvironmentError):
                environments.validate_environment("SUP-IOS-MIN-001", value)

    def test_duplicate_json_members_are_rejected_at_every_depth(self) -> None:
        for value in ('{"id":"a","id":"a"}', '{"environment":{"ios":"27.0","ios":"27.0"}}',
                      '[{"id":"a","environment":{},"environment":{}}]'):
            with self.subTest(value=value), self.assertRaisesRegex(environments.EnvironmentError, "duplicate JSON member"):
                json.loads(value, object_pairs_hook=environments.reject_duplicate_members)
        self.assertEqual(json.loads('{"ios":"27.0","xcode":"27.0"}', object_pairs_hook=environments.reject_duplicate_members),
                         {"ios": "27.0", "xcode": "27.0"})

    def test_required_matrix_ids_match_supported_profiles(self) -> None:
        matrix = json.loads((Path(__file__).resolve().parents[1] / "conformance/support-matrix.json").read_text(),
                            object_pairs_hook=environments.reject_duplicate_members)
        required = {cell["id"] for cell in matrix["cells"] if cell["policy"] == "required"}
        self.assertEqual(required, environments.REQUIRED_IDS)
        environments.validate_records(valid_records(), required)
        for bad in (required - {"SUP-IOS-MIN-001"}, required | {"SUP-MACOS-CURRENT-001"}):
            with self.subTest(required=bad), self.assertRaises(environments.EnvironmentError):
                environments.validate_records(valid_records(), bad, complete=False)


if __name__ == "__main__":
    unittest.main()
