"""Measure retained package environments; this does not establish release acceptance."""

from __future__ import annotations

import argparse
import hashlib
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
import xml.etree.ElementTree as ET


# Both module and direct-script execution use the repository-local validator.
sys.path.insert(0, str(Path(__file__).resolve().parent))
import support_environments


RN_IOS_CELL = "SUP-RN-IOS-CURRENT-001"
RN_IOS_CELLS = frozenset({"SUP-RN-IOS-MIN-001", RN_IOS_CELL})
IOS_CELLS = frozenset({"SUP-IOS-MIN-001", "SUP-IOS-CURRENT-001"}) | RN_IOS_CELLS
UDID = re.compile(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}")
COMMAND_TIMEOUT_SECONDS = 30
PG_CELL = "SUP-PG-LINUX-X64-001"
RN_ANDROID_CELL = "SUP-RN-ANDROID-CURRENT-001"
RN_ANDROID_CELLS = frozenset({"SUP-RN-ANDROID-MIN-001", RN_ANDROID_CELL})
ANDROID_CELLS = frozenset({"SUP-ANDROID-MIN-001", "SUP-ANDROID-CURRENT-001"}) | RN_ANDROID_CELLS
PROC_ROOT = Path("/proc")
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


def run_command(command: list[str], label: str, *, env: dict[str, str] | None = None, raw_output: bool = False) -> str:
    try:
        options = {} if env is None else {"env": env}
        result = subprocess.run(command, capture_output=True, text=not raw_output, check=True, timeout=COMMAND_TIMEOUT_SECONDS, **options)
        stdout = result.stdout.decode("utf-8") if raw_output else result.stdout
    except subprocess.TimeoutExpired:
        raise ProbeError(f"{label} timed out after 30 seconds") from None
    except (subprocess.CalledProcessError, OSError, UnicodeError):
        raise ProbeError(f"{label} command failed") from None
    if not isinstance(stdout, str):
        raise ProbeError(f"{label} returned invalid text")
    return stdout


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
    if (cell_id in RN_IOS_CELLS) != (react_native_app is not None):
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


def checked_android_text(value: object, label: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip() or any(ord(char) < 32 or ord(char) == 127 for char in value):
        raise ProbeError(f"{label} is missing or malformed")
    return value


def android_serial(value: object) -> str:
    value = checked_android_text(value, "Android serial")
    match = re.fullmatch(r"emulator-([1-9][0-9]*)", value)
    if match is None or not 1 <= int(match[1]) <= 65535:
        raise ProbeError("Android serial must identify an emulator console port")
    return value


def inherited_android_serial(requested: str | None = None) -> str | None:
    values = [android_serial(value) for name in ("ANDROID_SERIAL", "KOTLIN_ANDROID_SERIAL") if (value := os.environ.get(name))]
    if len(set(values)) > 1 or (requested is not None and any(value != android_serial(requested) for value in values)):
        raise ProbeError("original Android serial values disagree with the retained serial")
    return android_serial(requested) if requested is not None else (values[0] if values else None)


def android_command(adb: Path, serial: str | None, arguments: list[str], label: str) -> str:
    inherited_android_serial(serial)
    return run_command([str(adb), "-L", "tcp:127.0.0.1:5037", *([] if serial is None else ["-s", serial]), *arguments], label)


def resolve_android_serial(sdk_root: Path, requested: str | None = None) -> str:
    selected = inherited_android_serial(requested)
    adb = sdk_root / "platform-tools/adb"
    try:
        executable = adb.is_file() and os.access(adb, os.X_OK)
    except OSError:
        executable = False
    if not executable:
        raise ProbeError("selected SDK must contain executable platform-tools/adb")
    lines = android_command(adb, None, ["devices", "-l"], "Android device enumeration").splitlines()
    if not lines or lines[0] != "List of devices attached":
        raise ProbeError("Android device enumeration is malformed")
    devices = {}
    for line in lines[1:]:
        if not line:
            continue
        fields = line.split()
        if len(fields) < 2 or fields[1] not in {"device", "offline", "unauthorized", "recovery", "sideload", "bootloader"} or any(not re.fullmatch(r"[^\s:]+:[^\s]+", field) for field in fields[2:]):
            raise ProbeError("Android device record is malformed")
        checked_android_text(fields[0], "listed Android serial")
        if fields[0] in devices:
            raise ProbeError("Android device serial is duplicated")
        if fields[0].startswith("emulator-"):
            android_serial(fields[0])
        devices[fields[0]] = fields[1]
    if selected is None:
        if len(devices) != 1:
            raise ProbeError("Android resolution requires exactly one listed device")
        selected = android_serial(next(iter(devices)))
    if devices.get(selected) != "device":
        raise ProbeError("retained Android device must be listed exactly once and online")
    return selected


def android_console_value(text: str, label: str) -> str:
    lines = text.splitlines()
    if len(lines) != 2 or lines[1] != "OK":
        raise ProbeError(f"{label} console response is malformed")
    return checked_android_text(lines[0], label)


def android_stat(path: Path) -> dict[str, int]:
    stat = path.stat()
    return {"device": stat.st_dev, "inode": stat.st_ino, "size": stat.st_size, "mtime_ns": stat.st_mtime_ns}


def android_metadata(path: Path) -> tuple[str, dict[str, object]]:
    before = android_stat(path)
    content = path.read_bytes()
    if android_stat(path) != before or len(content) != before["size"]:
        raise ProbeError("Android metadata changed while being read")
    return content.decode("utf-8"), {**before, "sha256": hashlib.sha256(content).hexdigest()}


def android_ini(text: str) -> dict[str, str]:
    values = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith(("#", ";")):
            continue
        if "=" not in line:
            raise ProbeError("Android INI metadata is malformed")
        key, value = (part.strip() for part in line.split("=", 1))
        checked_android_text(key, "Android INI key")
        if key in values:
            raise ProbeError("Android INI key is duplicated")
        values[key] = value
    return values


def android_package(text: str, expected: str, *, image: bool = False) -> str:
    try:
        root = ET.fromstring(text)
    except ET.ParseError:
        raise ProbeError("Android package XML is malformed") from None
    packages = [element for element in root.iter() if element.tag.split("}")[-1] == "localPackage"]
    if len(packages) != 1 or packages[0].get("path") != expected:
        raise ProbeError("Android package must identify the exact installed directory")
    revisions = [element for element in packages[0] if element.tag.split("}")[-1] == "revision"]
    if len(revisions) != 1:
        raise ProbeError("Android package revision is missing or duplicated")
    components = {}
    for element in revisions[0]:
        name = element.tag.split("}")[-1]
        if name not in {"major", "minor", "micro", "preview"} or name in components or not re.fullmatch(r"0|[1-9][0-9]*", element.text or ""):
            raise ProbeError("Android package revision is malformed or duplicated")
        components[name] = element.text
    if "major" not in components:
        raise ProbeError("Android package revision is missing")
    if image:
        if components["major"] == "0" or any(components.get(name, "0") != "0" for name in ("minor", "micro", "preview")):
            raise ProbeError("Android image revision must be a positive integer")
        return components["major"]
    if set(components) != {"major", "minor", "micro"}:
        raise ProbeError("Android emulator package requires explicit major, minor, and micro without preview")
    return ".".join(components[name] for name in ("major", "minor", "micro"))


def android_socket_inodes(port: int) -> list[int]:
    inodes = set()
    for name in ("tcp", "tcp6"):
        lines = (PROC_ROOT / "net" / name).read_text(encoding="utf-8").splitlines()
        if not lines or "local_address" not in lines[0]:
            raise ProbeError("Linux socket metadata is malformed")
        for line in lines[1:]:
            fields = line.split()
            address_digits = 8 if name == "tcp" else 32
            if len(fields) < 10 or not re.fullmatch(rf"[0-9A-Fa-f]{{{address_digits}}}:[0-9A-Fa-f]{{4}}", fields[1]) or not re.fullmatch(r"[0-9A-Fa-f]{2}", fields[3]):
                raise ProbeError("Linux socket record is malformed")
            if int(fields[1].rsplit(":", 1)[1], 16) == port and fields[3].upper() == "0A":
                if not re.fullmatch(r"[1-9][0-9]*", fields[9]):
                    raise ProbeError("Linux listening socket inode is malformed")
                inodes.add(int(fields[9]))
    if not inodes:
        raise ProbeError("retained emulator has no listening console socket")
    return sorted(inodes)


def android_process(sdk_root: Path, serial: str, avd_name: str) -> tuple[dict[str, object], list[int]]:
    inodes = android_socket_inodes(int(serial.split("-")[1]))
    owners = {}
    for process in PROC_ROOT.iterdir():
        if not re.fullmatch(r"[1-9][0-9]*", process.name):
            continue
        owned: set[int] = set()
        try:
            if process.stat().st_uid != os.getuid():
                continue
            for fd in (process / "fd").iterdir():
                try:
                    link = os.readlink(fd)
                except PermissionError:
                    text = run_command(["sudo", "--non-interactive", "readlink", "--", str(fd)], "Android descriptor link", raw_output=True)
                    if not text.endswith("\n"):
                        raise ProbeError("Android descriptor link is malformed")
                    link = text[:-1]
                    if not link or any(ord(char) < 32 or 127 <= ord(char) <= 159 for char in link):
                        raise ProbeError("Android descriptor link is malformed")
                except FileNotFoundError:
                    continue
                if link in {f"socket:[{inode}]" for inode in inodes}:
                    owned.add(int(link[8:-1]))
            if owned:
                owners[process] = owned
        except FileNotFoundError:
            # Only an unrelated disappearing process can be ignored.
            if owned:
                raise ProbeError("selected emulator process disappeared") from None
            continue
    if len(owners) != 1 or next(iter(owners.values()), set()) != set(inodes):
        raise ProbeError("console sockets must have exactly one same-user process owner")
    process = next(iter(owners))
    pid = int(process.name)
    stat = (process / "stat").read_text(encoding="utf-8")
    match = re.fullmatch(r"([1-9][0-9]*) \((.*)\) (.+)\n?", stat)
    fields = match[3].split() if match is not None else []
    if match is None or int(match[1]) != pid or len(fields) < 20 or not re.fullmatch(r"0|[1-9][0-9]*", fields[19]):
        raise ProbeError("emulator process start time is malformed")
    arguments = (process / "cmdline").read_bytes().decode("utf-8").split("\x00")
    selectors = []
    for index, argument in enumerate(arguments):
        if argument == "-avd":
            selectors.append(arguments[index + 1] if index + 1 < len(arguments) else "")
        elif argument.startswith("@"):
            selectors.append(argument[1:])
    if selectors != [avd_name]:
        raise ProbeError("emulator process must select the exact console AVD once")
    link = checked_android_text(os.readlink(process / "exe"), "emulator executable path")
    if link.endswith(" (deleted)"):
        raise ProbeError("emulator executable was deleted")
    executable = Path(link).resolve(strict=True)
    engines = sdk_root / "emulator/qemu/linux-x86_64"
    if executable not in {engines / "qemu-system-x86_64", engines / "qemu-system-x86_64-headless"}:
        raise ProbeError("emulator executable is outside the permitted selected SDK engines")
    installed = android_stat(executable)
    if android_stat(process / "exe") != installed:
        raise ProbeError("executing emulator differs from its installed executable")
    return {"pid": pid, "start_time": fields[19], "executable": {"path": str(executable), **installed}}, inodes


def android_binary_version(text: str) -> tuple[str, str, str]:
    lines = [line for line in text.splitlines() if line.startswith("Android emulator version ")]
    if len(lines) != 1:
        raise ProbeError("emulator binary version line is missing or duplicated")
    line = checked_android_text(lines[0], "emulator binary version line")
    component = r"(?:0|[1-9][0-9]*)"
    match = re.fullmatch(rf"Android emulator version ({component}\.{component}\.{component})(?:\.0)? \(build_id ([1-9][0-9]*)\)(?: \(CL:[^()\r\n]+\))?", line)
    if match is None:
        raise ProbeError("emulator binary version or build is malformed")
    return match[1], match[2], line


def probe_android(cell_id: str, sdk_root: Path, serial: str, output: Path, identity_output: Path,
                  react_native_app: Path | None = None, initial_identity: Path | None = None,
                  initial_environment: Path | None = None) -> dict[str, object]:
    if cell_id not in ANDROID_CELLS or (cell_id in RN_ANDROID_CELLS) != (react_native_app is not None):
        raise ProbeError("React Native app is required only for the React Native Android cell")
    if (initial_identity is None) != (initial_environment is None) or output.resolve() == identity_output.resolve():
        raise ProbeError("Android output paths must differ and initial records must be supplied together")
    inherited_android_serial(serial)
    try:
        release = platform.freedesktop_os_release()
        if platform.system() != "Linux" or platform.machine() != "x86_64" or not isinstance(release, dict) or release.get("ID") != "ubuntu" or release.get("VERSION_ID") != "24.04":
            raise ProbeError("Android support cells require Linux x86_64 Ubuntu 24.04")
        sdk_root = sdk_root.resolve(strict=True)
        serial = resolve_android_serial(sdk_root, serial)
        adb = sdk_root / "platform-tools/adb"
        api = checked_android_text(android_command(adb, serial, ["shell", "getprop", "ro.build.version.sdk"], "Android guest API").removesuffix("\n").removesuffix("\r"), "Android guest API")
        name = android_console_value(android_command(adb, serial, ["emu", "avd", "name"], "Android AVD name"), "Android AVD name")
        avd = Path(android_console_value(android_command(adb, serial, ["emu", "avd", "path"], "Android AVD path"), "Android AVD path")).resolve(strict=True)
        process, sockets = android_process(sdk_root, serial, name)
        metadata = {}
        def read(path: Path) -> str:
            text, identity = android_metadata(path)
            metadata[str(path)] = identity
            return text
        config = android_ini(read(avd / "config.ini"))
        runtime = android_ini(read(avd / "hardware-qemu.ini"))
        image_path = Path(checked_android_text(config.get("image.sysdir.1"), "configured Android system image"))
        image_path = (image_path if image_path.is_absolute() else sdk_root / image_path).resolve(strict=True)
        if not image_path.is_relative_to((sdk_root / "system-images").resolve(strict=True)) or image_path == sdk_root / "system-images":
            raise ProbeError("Android image must be within the selected SDK system-images directory")
        partition = Path(checked_android_text(runtime.get("disk.systemPartition.initPath"), "runtime Android system image")).resolve(strict=True)
        if runtime.get("avd.name") != name or not partition.is_relative_to(image_path) or partition == image_path:
            raise ProbeError("Android runtime AVD or system image does not match its configuration")
        image_package = ";".join(image_path.relative_to(sdk_root).parts)
        revision = android_package(read(image_path / "package.xml"), image_package, image=True)
        installed_version = android_package(read(sdk_root / "emulator/package.xml"), "emulator")
        version, build, version_line = android_binary_version(run_command([str(PROC_ROOT / str(process["pid"]) / "exe"), "-version"], "emulator binary version", env={**os.environ, "LD_LIBRARY_PATH": f"{sdk_root}/emulator/lib64:{sdk_root}/emulator/lib64/qt/lib"}))
        if version != installed_version:
            raise ProbeError("installed and executing emulator versions differ")
        environment = {"android_api": api, "os": f"{release['ID']}-{release['VERSION_ID']}", "system_image": image_package, "system_image_revision": revision, "emulator_version": version, "emulator_build": build}
        if react_native_app is not None:
            environment["react_native"] = installed_react_native(react_native_app)
        measured = support_environments.validate_environment(cell_id, environment)
        record = {"id": cell_id, "environment": measured}
        identity = {"format_version": 1, "id": cell_id, "serial": serial, "sdk_root": checked_android_text(str(sdk_root), "SDK path"), **process, "avd": {"name": name, "path": checked_android_text(str(avd), "AVD path"), "system_image_path": checked_android_text(str(partition), "runtime system image path")}, "socket_inodes": sockets, "metadata": metadata, "binary_version_line": version_line}
        if resolve_android_serial(sdk_root, serial) != serial or android_process(sdk_root, serial, name) != (process, sockets):
            raise ProbeError("Android device or process identity changed during measurement")
        for path, before in metadata.items():
            if android_metadata(Path(path))[1] != before:
                raise ProbeError("Android installed metadata changed during measurement")
        if initial_identity is not None:
            initial = parse_metadata(initial_identity.read_text(encoding="utf-8"), "initial Android identity")
            previous = parse_metadata(initial_environment.read_text(encoding="utf-8"), "initial Android environment")
            if not isinstance(previous, dict) or set(previous) != {"id", "environment"} or previous["id"] != cell_id:
                raise ProbeError("initial Android environment record is malformed")
            support_environments.validate_environment(cell_id, previous["environment"])
            # Canonical JSON equality also distinguishes booleans from integer identities.
            if json.dumps(initial, sort_keys=True) != json.dumps(identity, sort_keys=True) or previous != record:
                raise ProbeError("Android identity or environment changed before resume")
    except ProbeError:
        raise
    except (OSError, UnicodeError, ValueError, support_environments.EnvironmentError):
        raise ProbeError("Android process or installed metadata is unreadable or malformed") from None
    write_record(identity_output, identity)
    write_record(output, record)
    print(version_line, file=sys.stderr)
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
    resolve_android = commands.add_parser("resolve-android-serial")
    resolve_android.add_argument("--sdk-root", required=True, type=Path)
    android = commands.add_parser("android")
    android.add_argument("--cell", required=True, choices=sorted(ANDROID_CELLS))
    android.add_argument("--sdk-root", required=True, type=Path)
    android.add_argument("--serial", required=True)
    android.add_argument("--output", required=True, type=Path)
    android.add_argument("--identity-output", required=True, type=Path)
    android.add_argument("--react-native-app", type=Path)
    android.add_argument("--initial-identity", type=Path)
    android.add_argument("--initial-environment", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        if args.command == "ios":
            probe_ios(args.cell, args.simulator_udid, args.output, args.react_native_app)
        elif args.command == "postgresql":
            probe_postgresql(args.cell, args.pg18_bindir, args.output)
        elif args.command == "resolve-android-serial":
            print(resolve_android_serial(args.sdk_root))
        else:
            probe_android(args.cell, args.sdk_root, args.serial, args.output, args.identity_output, args.react_native_app, args.initial_identity, args.initial_environment)
    except ProbeError as error:
        print(f"environment probe failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
