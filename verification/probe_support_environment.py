"""Measure a retained Apple simulator; this does not establish release acceptance."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
from typing import Any
from uuid import UUID


# Both module and direct-script execution use the repository-local validator.
sys.path.insert(0, str(Path(__file__).resolve().parent))
import support_environments


IOS_CELLS = frozenset({"SUP-IOS-MIN-001", "SUP-IOS-CURRENT-001", "SUP-RN-IOS-CURRENT-001"})
RN_IOS_CELL = "SUP-RN-IOS-CURRENT-001"
UDID = re.compile(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}")
COMMAND_TIMEOUT_SECONDS = 30


class ProbeError(ValueError):
    """A bounded command or metadata failure prevented measurement."""


def device_identity(value: object) -> UUID:
    if not isinstance(value, str) or not UDID.fullmatch(value):
        raise ProbeError("simulator UDID must be a UUID")
    return UUID(value)


def run_command(command: list[str], label: str) -> str:
    try:
        result = subprocess.run(command, capture_output=True, text=True, check=True, timeout=COMMAND_TIMEOUT_SECONDS)
    except subprocess.TimeoutExpired:
        raise ProbeError(f"{label} timed out after 30 seconds") from None
    except (subprocess.CalledProcessError, OSError, UnicodeError):
        raise ProbeError(f"{label} command failed") from None
    if not isinstance(result.stdout, str):
        raise ProbeError(f"{label} returned invalid text")
    return result.stdout


def parse_metadata(text: str, label: str) -> Any:
    try:
        return json.loads(text, object_pairs_hook=support_environments.reject_duplicate_members)
    except (json.JSONDecodeError, support_environments.EnvironmentError):
        raise ProbeError(f"{label} metadata is malformed or duplicated") from None


def xcode_metadata(text: str) -> tuple[str, str]:
    lines = [line for line in text.splitlines() if line]
    if len(lines) != 2 or not lines[0].startswith("Xcode ") or not lines[1].startswith("Build version "):
        raise ProbeError("Xcode version and build metadata are missing or malformed")
    version, build = lines[0][len("Xcode "):], lines[1][len("Build version "):]
    if not build.strip() or len(build) > 200 or any(ord(character) < 32 for character in build):
        raise ProbeError("Xcode build metadata is missing or malformed")
    return version, lines[1]


def simulator_version(devices: object, runtimes: object, requested: UUID) -> str:
    groups = devices.get("devices") if isinstance(devices, dict) else None
    if not isinstance(groups, dict):
        raise ProbeError("booted device metadata is missing or malformed")
    matches = []
    for runtime_id, entries in groups.items():
        if not isinstance(runtime_id, str) or not runtime_id or not isinstance(entries, list):
            raise ProbeError("booted device metadata is missing or malformed")
        for device in entries:
            if not isinstance(device, dict):
                raise ProbeError("booted device metadata is missing or malformed")
            if device_identity(device.get("udid")) == requested:
                matches.append((runtime_id, device))
    if len(matches) != 1:
        raise ProbeError("requested simulator must occur exactly once in booted device metadata")
    runtime_id, device = matches[0]
    if device.get("state") != "Booted" or device.get("isAvailable") is not True:
        raise ProbeError("requested simulator must be booted and available")
    entries = runtimes.get("runtimes") if isinstance(runtimes, dict) else None
    if not isinstance(entries, list) or any(not isinstance(runtime, dict) for runtime in entries):
        raise ProbeError("simulator runtime metadata is missing or malformed")
    matching = [runtime for runtime in entries if runtime.get("identifier") == runtime_id]
    if len(matching) != 1:
        raise ProbeError("requested simulator runtime must resolve exactly once")
    runtime = matching[0]
    if runtime.get("isAvailable") is not True or runtime.get("platform") != "iOS":
        raise ProbeError("requested simulator runtime must be available iOS")
    version = runtime.get("version")
    if not isinstance(version, str) or not version:
        raise ProbeError("simulator runtime version is missing or malformed")
    return version


def installed_react_native(app: Path) -> object:
    try:
        text = (app / "node_modules/react-native/package.json").read_text(encoding="utf-8")
    except (OSError, UnicodeError):
        raise ProbeError("installed React Native metadata is unreadable") from None
    package = parse_metadata(text, "installed React Native")
    if not isinstance(package, dict) or "version" not in package:
        raise ProbeError("installed React Native version metadata is missing")
    return package["version"]


def write_record(output: Path, record: dict[str, object]) -> None:
    temporary = None
    try:
        output.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=output.parent, prefix=f".{output.name}.", delete=False) as stream:
            temporary = Path(stream.name)
            stream.write(json.dumps(record, indent=2, sort_keys=True) + "\n")
        os.replace(temporary, output)
    except (OSError, UnicodeError):
        raise ProbeError("measured environment output could not be replaced") from None
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def probe_ios(cell_id: str, simulator_udid: str, output: Path, react_native_app: Path | None = None) -> dict[str, object]:
    if cell_id not in IOS_CELLS:
        raise ProbeError("unsupported Apple support cell")
    requested = device_identity(simulator_udid)
    if (cell_id == RN_IOS_CELL) != (react_native_app is not None):
        raise ProbeError("React Native app is required only for the React Native iOS cell")
    xcode, build_line = xcode_metadata(run_command(["xcodebuild", "-version"], "Xcode version"))
    devices = parse_metadata(run_command(["xcrun", "simctl", "list", "devices", "booted", "-j"], "booted simulator devices"), "booted simulator devices")
    runtimes = parse_metadata(run_command(["xcrun", "simctl", "list", "runtimes", "-j"], "simulator runtimes"), "simulator runtimes")
    environment = {"ios": simulator_version(devices, runtimes, requested), "xcode": xcode}
    if react_native_app is not None:
        environment["react_native"] = installed_react_native(react_native_app)
    try:
        measured = support_environments.validate_environment(cell_id, environment)
    except support_environments.EnvironmentError:
        raise ProbeError("measured Apple environment violates the supported profile") from None
    record = {"id": cell_id, "environment": measured}
    write_record(output, record)
    # Retain only the checked version/build diagnostics, never arbitrary command output.
    print(f"Xcode {measured['xcode']}\n{build_line}", file=sys.stderr)
    return record


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    ios = commands.add_parser("ios")
    ios.add_argument("--cell", required=True, choices=sorted(IOS_CELLS))
    ios.add_argument("--simulator-udid", required=True)
    ios.add_argument("--output", required=True, type=Path)
    ios.add_argument("--react-native-app", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        probe_ios(args.cell, args.simulator_udid, args.output, args.react_native_app)
    except ProbeError as error:
        print(f"environment probe failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
