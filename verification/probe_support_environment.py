"""Measure retained package environments; this does not establish release acceptance."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import platform
import re
import subprocess
import sys
import tempfile
from typing import Any
from urllib.parse import parse_qsl, unquote, urlsplit
from uuid import UUID


# Both module and direct-script execution use the repository-local validator.
sys.path.insert(0, str(Path(__file__).resolve().parent))
import support_environments


IOS_CELLS = frozenset({"SUP-IOS-MIN-001", "SUP-IOS-CURRENT-001", "SUP-RN-IOS-CURRENT-001"})
RN_IOS_CELL = "SUP-RN-IOS-CURRENT-001"
UDID = re.compile(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}")
COMMAND_TIMEOUT_SECONDS = 30
PG_CELL = "SUP-PG-LINUX-X64-001"
PG_VERSION_QUERY = """SELECT pg_catalog.json_build_object(
    'version', pg_catalog.current_setting('server_version'),
    'version_num', pg_catalog.current_setting('server_version_num')
)::text"""


class ProbeError(ValueError):
    """A bounded command or metadata failure prevented measurement."""


def device_identity(value: object) -> UUID:
    if not isinstance(value, str) or not UDID.fullmatch(value):
        raise ProbeError("simulator UDID must be a UUID")
    return UUID(value)


def run_command(command: list[str], label: str, *, env: dict[str, str] | None = None) -> str:
    try:
        options = {} if env is None else {"env": env}
        result = subprocess.run(command, capture_output=True, text=True, check=True, timeout=COMMAND_TIMEOUT_SECONDS, **options)
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


def postgresql_connection_environment() -> tuple[dict[str, str], dict[str, str]]:
    credentials = {name: os.environ.get(name) for name in ("PGDATABASE", "PGUSER", "PGPASSWORD")}
    if any(not isinstance(value, str) or not value or "\x00" in value for value in credentials.values()):
        raise ProbeError("PostgreSQL query requires nonempty libpq credentials")
    uri = credentials["PGDATABASE"]
    try:
        parsed = urlsplit(uri)
        valid = (
            not any(character.isspace() or ord(character) < 32 for character in uri)
            and not re.search(r"%(?![0-9a-fA-F]{2})", uri)
            and parsed.scheme in {"postgres", "postgresql"}
            and parsed.username is None and parsed.password is None
            and re.fullmatch(r"127\.0\.0\.1:[0-9]+", parsed.netloc) is not None
            and parsed.hostname == "127.0.0.1" and parsed.port is not None and 1 <= parsed.port <= 65535
            and parsed.path.startswith("/") and bool(unquote(parsed.path[1:]))
            and "\x00" not in unquote(parsed.path)
            and "#" not in uri
            and parse_qsl(parsed.query, keep_blank_values=True, strict_parsing=True) == [("sslmode", "disable")]
        )
    except ValueError:
        valid = False
    if not valid:
        raise ProbeError("PostgreSQL attach URI must use the credential-free local provisioner interface")
    clean = {name: value for name, value in os.environ.items() if not name.startswith("PG")}
    query = {**clean, "PGHOST": parsed.hostname, "PGPORT": str(parsed.port),
             "PGDATABASE": unquote(parsed.path[1:]), "PGSSLMODE": "disable",
             "PGUSER": credentials["PGUSER"], "PGPASSWORD": credentials["PGPASSWORD"],
             "PGCONNECT_TIMEOUT": "10",
             "PGOPTIONS": "-c default_transaction_read_only=on -c statement_timeout=10000"}
    return clean, query


def postgresql_version(text: str, *, binary: bool = False) -> str:
    if binary:
        prefix = "postgres (PostgreSQL) "
        if not text.startswith(prefix):
            raise ProbeError("PostgreSQL binary version metadata is malformed")
        text = text[len(prefix):].removesuffix("\n")
    match = re.fullmatch(r"(18\.(?:0|[1-9][0-9]*))(?: \(([^()\r\n]{1,200})\))?", text)
    if match is None or len(text.splitlines()) != 1 or (match[2] is not None and (not match[2].strip() or any(ord(character) < 32 or ord(character) == 127 for character in match[2]))):
        raise ProbeError("PostgreSQL version must be canonical 18.patch with an optional distribution suffix")
    return match[1]


def probe_postgresql(cell_id: str, pg18_bindir: Path, output: Path) -> dict[str, object]:
    if cell_id != PG_CELL:
        raise ProbeError("unsupported PostgreSQL support cell")
    binary_environment, query_environment = postgresql_connection_environment()
    try:
        system, architecture = platform.system(), platform.machine()
        release = platform.freedesktop_os_release()
    except (OSError, ValueError, UnicodeError):
        raise ProbeError("Linux OS-release metadata is unreadable") from None
    if system != "Linux" or architecture != "x86_64" or not isinstance(release, dict) or release.get("ID") != "ubuntu" or release.get("VERSION_ID") != "24.04":
        raise ProbeError("PostgreSQL support cell requires Linux x86_64 Ubuntu 24.04")
    postgres, psql = pg18_bindir / "postgres", pg18_bindir / "psql"
    try:
        executable = all(path.is_file() and os.access(path, os.X_OK) for path in (postgres, psql))
    except OSError:
        executable = False
    if not executable:
        raise ProbeError("retained PostgreSQL directory must contain executable postgres and psql files")
    installed = postgresql_version(run_command([str(postgres), "--version"], "PostgreSQL binary version", env=binary_environment), binary=True)
    server = parse_metadata(run_command(
        [str(psql), "-X", "-A", "-t", "--no-password", "-v", "ON_ERROR_STOP=1", "-c", PG_VERSION_QUERY],
        "PostgreSQL server version", env=query_environment,
    ), "PostgreSQL server version")
    if not isinstance(server, dict) or set(server) != {"version", "version_num"} or any(not isinstance(value, str) or not value for value in server.values()):
        raise ProbeError("PostgreSQL server version metadata must contain exactly two nonempty string fields")
    actual = postgresql_version(server["version"])
    if installed != actual:
        raise ProbeError("retained PostgreSQL binary and running server versions differ")
    try:
        number = str(180000 + int(actual.split(".")[1]))
    except ValueError:
        raise ProbeError("PostgreSQL version metadata is malformed") from None
    if server["version_num"] != number:
        raise ProbeError("PostgreSQL server version number does not match its canonical version")
    try:
        measured = support_environments.validate_environment(cell_id, {
            "architecture": architecture, "os": f"{release['ID']}-{release['VERSION_ID']}", "postgresql": actual,
        })
    except support_environments.EnvironmentError:
        raise ProbeError("measured PostgreSQL environment violates the supported profile") from None
    record = {"id": cell_id, "environment": measured}
    write_record(output, record)
    print(f"PostgreSQL {actual}; Linux {architecture}; {measured['os']}", file=sys.stderr)
    return record


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    ios = commands.add_parser("ios")
    ios.add_argument("--cell", required=True, choices=sorted(IOS_CELLS))
    ios.add_argument("--simulator-udid", required=True)
    ios.add_argument("--output", required=True, type=Path)
    ios.add_argument("--react-native-app", type=Path)
    postgresql = commands.add_parser("postgresql")
    postgresql.add_argument("--cell", required=True, choices=[PG_CELL])
    postgresql.add_argument("--pg18-bindir", required=True, type=Path)
    postgresql.add_argument("--output", required=True, type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        if args.command == "ios":
            probe_ios(args.cell, args.simulator_udid, args.output, args.react_native_app)
        else:
            probe_postgresql(args.cell, args.pg18_bindir, args.output)
    except ProbeError as error:
        print(f"environment probe failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
