"""Validate sealed environment shapes; this does not establish current-stable authority."""

from __future__ import annotations

import re
from typing import Any


ANDROID_FIELDS = frozenset({
    "android_api", "os", "system_image", "system_image_revision",
    "emulator_version", "emulator_build",
})
CELL_FIELDS = {
    "SUP-PG-LINUX-X64-001": frozenset({"architecture", "os", "postgresql"}),
    "SUP-IOS-MIN-001": frozenset({"ios", "xcode"}),
    "SUP-IOS-CURRENT-001": frozenset({"ios", "xcode"}),
    "SUP-RN-IOS-MIN-001": frozenset({"ios", "xcode", "react_native"}),
    "SUP-RN-IOS-CURRENT-001": frozenset({"ios", "xcode", "react_native"}),
    "SUP-ANDROID-MIN-001": ANDROID_FIELDS,
    "SUP-ANDROID-CURRENT-001": ANDROID_FIELDS,
    "SUP-RN-ANDROID-MIN-001": ANDROID_FIELDS | {"react_native"},
    "SUP-RN-ANDROID-CURRENT-001": ANDROID_FIELDS | {"react_native"},
}
REQUIRED_IDS = frozenset(CELL_FIELDS)
RN_MIN_CELLS = frozenset({"SUP-RN-IOS-MIN-001", "SUP-RN-ANDROID-MIN-001"})
COMPONENT = r"(?:0|[1-9][0-9]*)"
POSITIVE_INTEGER = re.compile(r"[1-9][0-9]*")
APPLE_VERSION = re.compile(rf"{COMPONENT}\.{COMPONENT}(?:\.{COMPONENT})?")
THREE_COMPONENT_VERSION = re.compile(rf"{COMPONENT}\.{COMPONENT}\.{COMPONENT}")
POSTGRESQL_VERSION = re.compile(rf"18\.{COMPONENT}")
SYSTEM_IMAGE = re.compile(r"system-images;android-([1-9][0-9]*)(?:\.0)?;google_apis;x86_64")
UNRESOLVED = re.compile(r"current|latest|stable|(?:^|\.)x(?:\.|$)|\*", re.IGNORECASE)


class EnvironmentError(ValueError):
    """A value violates the closed sealed environment contract."""


def reject_duplicate_members(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for name, value in pairs:
        if name in result:
            raise EnvironmentError(f"duplicate JSON member: {name}")
        result[name] = value
    return result


def version_components(value: str) -> tuple[int, ...]:
    return tuple(int(component) for component in value.split("."))


def validate_environment(cell_id: object, value: object) -> dict[str, str]:
    if not isinstance(cell_id, str) or cell_id not in CELL_FIELDS:
        raise EnvironmentError(f"unknown support cell: {cell_id!r}")
    if not isinstance(value, dict) or set(value) != CELL_FIELDS[cell_id]:
        raise EnvironmentError(f"cell {cell_id} environment has missing or unknown fields")
    for field, item in value.items():
        if not isinstance(item, str) or not item:
            raise EnvironmentError(f"cell {cell_id} field {field} must be a nonempty string")
        if UNRESOLVED.search(item):
            raise EnvironmentError(f"cell {cell_id} field {field} contains an unresolved selector")

    if "os" in value and value["os"] != "ubuntu-24.04":
        raise EnvironmentError(f"cell {cell_id} OS must be ubuntu-24.04")
    if "postgresql" in value:
        if value["architecture"] != "x86_64" or not POSTGRESQL_VERSION.fullmatch(value["postgresql"]):
            raise EnvironmentError(f"cell {cell_id} requires x86_64 and canonical PostgreSQL 18.patch")
    if "ios" in value:
        if not APPLE_VERSION.fullmatch(value["ios"]) or not APPLE_VERSION.fullmatch(value["xcode"]):
            raise EnvironmentError(f"cell {cell_id} requires canonical Apple versions")
        major = version_components(value["ios"])[0]
        if major < 17 or (cell_id == "SUP-IOS-MIN-001" and major != 17):
            raise EnvironmentError(f"cell {cell_id} iOS version violates the supported minimum")
    if "android_api" in value:
        for field in ("android_api", "system_image_revision", "emulator_build"):
            if not POSITIVE_INTEGER.fullmatch(value[field]):
                raise EnvironmentError(f"cell {cell_id} field {field} must be a positive integer string")
        api = int(value["android_api"])
        if api < 24 or (cell_id == "SUP-ANDROID-MIN-001" and api != 24):
            raise EnvironmentError(f"cell {cell_id} Android API violates the supported minimum")
        image = SYSTEM_IMAGE.fullmatch(value["system_image"])
        if image is None or image[1] != value["android_api"]:
            raise EnvironmentError(f"cell {cell_id} system image must match its device API and x86_64 profile")
        if not THREE_COMPONENT_VERSION.fullmatch(value["emulator_version"]):
            raise EnvironmentError(f"cell {cell_id} emulator version must have three canonical components")
    if "react_native" in value:
        if not THREE_COMPONENT_VERSION.fullmatch(value["react_native"]):
            raise EnvironmentError(f"cell {cell_id} requires a canonical React Native version")
        runtime = version_components(value["react_native"])
        if cell_id in RN_MIN_CELLS:
            if runtime != (0, 82, 1):
                raise EnvironmentError(f"cell {cell_id} React Native must use 0.82.1")
        else:
            if runtime[:2] != (0, 83):
                raise EnvironmentError(f"cell {cell_id} React Native must use the supported 0.83 series")
            if "xcode" in value and version_components(value["xcode"]) >= (26, 4) and runtime[2] < 5:
                raise EnvironmentError(f"cell {cell_id} Xcode 26.4 or later requires React Native 0.83.5 or later")
    return dict(sorted(value.items()))


def validate_required_ids(required_ids: set[str] | frozenset[str]) -> None:
    if required_ids != REQUIRED_IDS:
        raise EnvironmentError("support matrix required IDs do not match the nine supported profiles")


def validate_records(
    value: object,
    required_ids: set[str] | frozenset[str] = REQUIRED_IDS,
    *,
    complete: bool = True,
) -> list[dict[str, Any]]:
    """Incomplete mode is only for diagnostics; every present environment stays complete."""
    validate_required_ids(required_ids)
    if not isinstance(value, list):
        raise EnvironmentError("support environments must be an array")
    records: dict[str, dict[str, str]] = {}
    for record in value:
        if not isinstance(record, dict) or set(record) != {"id", "environment"}:
            raise EnvironmentError("support environments contain a malformed record")
        cell_id = record["id"]
        environment = validate_environment(cell_id, record["environment"])
        if cell_id in records:
            raise EnvironmentError(f"duplicate support cell: {cell_id}")
        records[cell_id] = environment
    if complete and set(records) != required_ids:
        raise EnvironmentError("support environments must contain every required cell exactly once")
    for native, bridge, field in (
        ("SUP-IOS-CURRENT-001", "SUP-RN-IOS-CURRENT-001", "ios"),
        ("SUP-IOS-CURRENT-001", "SUP-RN-IOS-MIN-001", "ios"),
        ("SUP-ANDROID-CURRENT-001", "SUP-RN-ANDROID-CURRENT-001", "android_api"),
        ("SUP-ANDROID-CURRENT-001", "SUP-RN-ANDROID-MIN-001", "android_api"),
    ):
        if native in records and bridge in records and records[native][field] != records[bridge][field]:
            raise EnvironmentError(f"native and React Native current {field} versions differ")
    return [{"id": cell_id, "environment": records[cell_id]} for cell_id in sorted(records)]
