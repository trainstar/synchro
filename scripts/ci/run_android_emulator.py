from __future__ import annotations

import argparse
import hashlib
import os
from pathlib import Path, PurePosixPath
import platform
import re
import shutil
import signal
import socket
import stat
import subprocess
import sys
import tempfile
import time
import xml.etree.ElementTree as ET
import zipfile


ARCHIVE_URL = "https://dl.google.com/android/repository/emulator-linux_x64-16428233.zip"
ARCHIVE_SHA256 = "b08fc43d8608d2955f607f1b287beb525041e730086bdca3af152accac3af9c1"
ARCHIVE_SIZE = 349654171
METADATA_SHA256 = "89bde98be50241a1a804c0c68b91981ab619e585fdd3ddf3a50fea95f6c67a87"
VERSION = "37.2.12"
BUILD = "16428233"
SERIAL = "emulator-5554"
ADB_ENDPOINT = "tcp:127.0.0.1:5037"
METADATA = Path(__file__).with_name("android-emulator-package.xml")


class RunnerError(Exception):
    pass


class Cancelled(Exception):
    pass


def arguments(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sdk-root", required=True, type=Path)
    parser.add_argument("--sdkmanager", required=True, type=Path)
    parser.add_argument("--api", required=True, choices=("24", "34", "37"))
    parser.add_argument("--image-api", required=True, choices=("24", "34", "37.0"))
    parser.add_argument("--avd-name", required=True)
    parser.add_argument("--profile", required=True, choices=("pixel_2", "pixel_7"))
    parser.add_argument("--memory", choices=("6144",))
    parser.add_argument("--log-dir", required=True, type=Path)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    selected = parser.parse_args(argv)
    if selected.command[:1] != ["--"] or len(selected.command) < 2:
        parser.error("An explicit command is required after --")
    selected.command = selected.command[1:]
    if (selected.api, selected.image_api) not in (("24", "24"), ("34", "34"), ("37", "37.0")):
        parser.error("The platform and image API selections do not match")
    if not re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.-]*", selected.avd_name):
        parser.error("The AVD name contains an unsupported character")
    return selected


def digest(path: Path) -> str:
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def check_ports() -> None:
    for port in (5037, 5554, 5555):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as connection:
            connection.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                connection.bind(("127.0.0.1", port))
            except OSError as error:
                raise RunnerError(f"Android port {port} is occupied") from error


def check_active_emulator(directory: Path, command) -> None:
    for process in Path("/proc").iterdir():
        if not process.name.isdecimal():
            continue
        try:
            prefix, closing, suffix = (process / "stat").read_text().rpartition(")")
        except FileNotFoundError:
            continue
        fields = suffix.split()
        if (
            not closing or not prefix.startswith(process.name + " (")
            or not suffix.startswith(" ") or len(fields) < 7 or len(fields[0]) != 1
            or any(not re.fullmatch(r"-?[0-9]+", value) for value in fields[1:6])
            or not re.fullmatch(r"[0-9]+", fields[6])
        ):
            raise RunnerError(f"Malformed process stat data: {process / 'stat'}")
        if fields[0] == "Z" or int(fields[6]) & 0x00200000:
            continue
        try:
            executable = (process / "exe").readlink()
        except FileNotFoundError:
            continue
        except PermissionError:
            try:
                output = command(["sudo", "--non-interactive", "readlink", "--", str(process / "exe")], timeout=30)
            except (RunnerError, OSError, subprocess.TimeoutExpired):
                if not process.exists():
                    continue
                raise
            value = output.rstrip("\n")
            if not value or "\n" in value or not Path(value).is_absolute():
                raise RunnerError("The privileged executable inspection returned an invalid path")
            executable = Path(value)
        if executable.is_relative_to(directory):
            raise RunnerError(f"An executable from {directory} is active")


def group_alive(process: subprocess.Popen) -> bool:
    for entry in Path("/proc").iterdir():
        if not entry.name.isdecimal():
            continue
        try:
            fields = (entry / "stat").read_text().rsplit(")", 1)[1].split()
        except FileNotFoundError:
            continue
        if int(fields[2]) == process.pid and fields[0] != "Z":
            return True
    return False


def stop_group(process: subprocess.Popen) -> None:
    for action in (signal.SIGTERM, signal.SIGKILL):
        if group_alive(process):
            try:
                os.killpg(process.pid, action)
            except ProcessLookupError:
                pass
        deadline = time.monotonic() + 5
        while group_alive(process) and time.monotonic() < deadline:
            time.sleep(0.05)
        remaining = max(0, deadline - time.monotonic())
        try:
            process.wait(timeout=remaining)
        except subprocess.TimeoutExpired:
            continue
        if not group_alive(process):
            return
    raise RunnerError(f"Owned process group {process.pid} did not stop")


def check_properties(directory: Path) -> None:
    properties = {}
    for line in (directory / "source.properties").read_text().splitlines():
        if "=" in line:
            key, value = line.split("=", 1)
            properties[key.strip()] = value.strip()
    if properties.get("Pkg.Revision") != VERSION or properties.get("Pkg.BuildId") != BUILD:
        raise RunnerError("Emulator source.properties does not match the pin")
    if properties.get("Pkg.Path") != "emulator":
        raise RunnerError("Emulator source.properties has an incorrect package path")


def extract_archive(archive: Path, destination: Path) -> dict[str, tuple[int, str | None]]:
    members = {}
    with zipfile.ZipFile(archive) as source:
        for member in source.infolist():
            name = member.filename
            path = PurePosixPath(name)
            mode = member.external_attr >> 16
            kind = stat.S_IFMT(mode)
            directory = member.is_dir()
            if (
                not name or member.orig_filename != name or "\\" in name or path.is_absolute()
                or any(part in ("", ".", "..") for part in name.rstrip("/").split("/"))
                or path.parts[0] != "emulator" or str(path) != name.rstrip("/")
                or (not directory and len(path.parts) < 2)
                or member.create_system != 3
                or kind != (stat.S_IFDIR if directory else stat.S_IFREG)
                or mode & 0o7000 or not mode & stat.S_IRUSR
                or (directory and not mode & stat.S_IXUSR)
                or str(path) in members or str(path) == "emulator/package.xml"
            ):
                raise RunnerError(f"Unsafe emulator archive member: {name}")
            members[str(path)] = (member, stat.S_IMODE(mode))
        if not members:
            raise RunnerError("The emulator archive is empty")
        for name in members:
            for parent in PurePosixPath(name).parents:
                if str(parent) in members and not members[str(parent)][0].is_dir():
                    raise RunnerError(f"An archive file is a parent directory: {parent}")
        for name, (member, mode) in members.items():
            output = destination / name
            if member.is_dir():
                output.mkdir(parents=True, exist_ok=True)
            else:
                output.parent.mkdir(parents=True, exist_ok=True)
                with source.open(member) as incoming, output.open("xb") as outgoing:
                    shutil.copyfileobj(incoming, outgoing)
            output.chmod(mode)
    return {
        name: (mode, None if member.is_dir() else digest(destination / name))
        for name, (member, mode) in members.items()
    }


def verify_installation(sdk: Path, members: dict[str, tuple[int, str | None]], metadata: bytes) -> None:
    for name, (mode, expected) in members.items():
        path = sdk / name
        if any(parent.is_symlink() for parent in (path, *path.parents) if parent.is_relative_to(sdk)):
            raise RunnerError(f"Installed archive path changed: {name}")
        if stat.S_IMODE(path.stat().st_mode) != mode:
            raise RunnerError(f"Installed archive mode changed: {name}")
        if expected is None:
            if not path.is_dir():
                raise RunnerError(f"Installed archive directory changed: {name}")
        elif not path.is_file() or digest(path) != expected:
            raise RunnerError(f"Installed archive bytes changed: {name}")
    package = sdk / "emulator/package.xml"
    if package.is_symlink() or package.read_bytes() != metadata:
        raise RunnerError("Installed emulator package.xml changed")
    check_properties(sdk / "emulator")


class Runner:
    def __init__(self, selected: argparse.Namespace):
        self.selected = selected
        self.environment = dict(os.environ)
        self.cancel_signal = 0
        self.cleaning = False
        self.cleanup_failed = False
        self.emulator = None
        self.adb_server = None
        self.test = None
        self.command_process = None
        self.test_status = None
        self.log = None
        self.adb_server_log = None

    def cancel(self, number: int, _frame) -> None:
        self.cancel_signal = number
        if not self.cleaning:
            raise Cancelled()

    def command(self, command: list[str], *, timeout: float = 180, input: str | None = None) -> str:
        self.command_process = subprocess.Popen(
            command, env=self.environment, start_new_session=True,
            stdin=subprocess.PIPE if input is not None else subprocess.DEVNULL,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        )
        failure = None
        output = None
        try:
            deadline = time.monotonic() + timeout
            while True:
                self.check_adb_server()
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise subprocess.TimeoutExpired(command, timeout)
                try:
                    output, _ = self.command_process.communicate(input=input, timeout=min(1, remaining))
                    break
                except subprocess.TimeoutExpired:
                    input = None
                    if self.emulator is not None and self.emulator.poll() is not None:
                        raise RunnerError("The emulator exited during an Android command")
            self.log.write(output)
            self.log.flush()
            self.check_adb_server()
            status = self.command_process.returncode
            if status:
                raise RunnerError(f"Command failed with status {status}: {command[0]}")
        except BaseException as error:
            failure = error
            raise
        finally:
            self.cleaning = True
            try:
                try:
                    stop_group(self.command_process)
                except (RunnerError, OSError, ValueError) as error:
                    self.cleanup_failed = True
                    print(f"Android command cleanup failed: {error}", file=sys.stderr)
                    try:
                        self.log.write(f"Android command cleanup failed: {error}\n")
                    except (OSError, ValueError):
                        pass
                    if failure is None:
                        failure = error
                        raise
                finally:
                    if output is None:
                        try:
                            output, _ = self.command_process.communicate(timeout=5)
                            self.log.write(output)
                            self.log.flush()
                        except (OSError, ValueError, subprocess.TimeoutExpired) as error:
                            self.cleanup_failed = True
                            print(f"Android command output drain failed: {error}", file=sys.stderr)
                            try:
                                self.log.write(f"Android command output drain failed: {error}\n")
                            except (OSError, ValueError):
                                pass
                            if failure is None:
                                raise
            finally:
                self.command_process = None
                self.cleaning = False
        if self.cancel_signal:
            raise Cancelled()
        return output

    def prepare(self) -> None:
        selected = self.selected
        if platform.system() != "Linux" or platform.machine() != "x86_64":
            raise RunnerError("The emulator runner requires Linux x86_64")
        for name in ("ANDROID_SERIAL", "KOTLIN_ANDROID_SERIAL"):
            if self.environment.get(name, "") not in ("", SERIAL):
                raise RunnerError(f"{name} conflicts with the selected serial {SERIAL}")
        sdk = selected.sdk_root.resolve(strict=True)
        manager = selected.sdkmanager.resolve(strict=True)
        avdmanager = manager.with_name("avdmanager")
        adb = sdk / "platform-tools/adb"
        if not sdk.is_dir() or not manager.is_relative_to(sdk / "cmdline-tools"):
            raise RunnerError("The selected sdkmanager must be under the selected SDK cmdline-tools directory")
        for tool in (manager, avdmanager, adb):
            if not tool.is_file() or not os.access(tool, os.R_OK | os.X_OK):
                raise RunnerError(f"The selected Android tool is not readable and executable: {tool}")
        metadata = METADATA.read_bytes()
        ET.fromstring(metadata)
        if hashlib.sha256(metadata).hexdigest() != METADATA_SHA256:
            raise RunnerError("The emulator metadata asset does not match the pin")
        directory = sdk / "emulator"
        if directory.is_symlink() or (directory.exists() and not directory.is_dir()):
            raise RunnerError("The selected SDK emulator path must be a directory")
        selected.sdk_root = sdk
        selected.sdkmanager = manager
        selected.log_dir = selected.log_dir.resolve()
        selected.log_dir.mkdir(parents=True, exist_ok=True)
        self.log = (selected.log_dir / "runner.log").open("a", buffering=1)
        avd_home = selected.log_dir / "avd"
        avd_home.mkdir(exist_ok=True)
        self.environment.update({
            "ANDROID_HOME": str(sdk), "ANDROID_SDK_ROOT": str(sdk),
            "ANDROID_SERIAL": SERIAL, "KOTLIN_ANDROID_SERIAL": SERIAL,
            "ANDROID_AVD_HOME": str(avd_home), "ADB_SERVER_SOCKET": ADB_ENDPOINT,
            "PATH": os.pathsep.join((str(adb.parent), str(manager.parent), self.environment.get("PATH", ""))),
        })
        for variable in ("ANDROID_ADB_SERVER_ADDRESS", "ANDROID_ADB_SERVER_PORT"):
            self.environment.pop(variable, None)
        check_active_emulator(directory, self.command)
        check_ports()
        with tempfile.TemporaryDirectory(prefix=".synchro-emulator-", dir=sdk) as work:
            work = Path(work)
            archive = work / "emulator.zip"
            self.command([
                "curl", "--fail", "--location", "--proto", "=https", "--proto-redir", "=https",
                "--retry", "3", "--retry-all-errors",
                "--connect-timeout", "30", "--max-time", "180", "--output", str(archive), ARCHIVE_URL,
            ])
            if archive.stat().st_size != ARCHIVE_SIZE or digest(archive) != ARCHIVE_SHA256:
                raise RunnerError("The emulator archive size or SHA-256 does not match the pin")
            members = extract_archive(archive, work)
            check_properties(work / "emulator")
            (work / "emulator/package.xml").write_bytes(metadata)
            check_active_emulator(directory, self.command)
            if directory.exists():
                shutil.rmtree(directory)
            (work / "emulator").rename(directory)
            image = f"system-images;android-{selected.image_api};google_apis;x86_64"
            self.command([str(manager), f"--sdk_root={sdk}", f"platforms;android-{selected.image_api}", image], timeout=900)
            verify_installation(sdk, members, metadata)
        version = self.command([str(directory / "emulator"), "-version"])
        if not re.search(r"\bAndroid emulator version 37\.2\.12\b.*\(build_id 16428233\)", version):
            raise RunnerError("The emulator executable version or build does not match the pin")
        identity = f"Emulator SHA-256 {ARCHIVE_SHA256}, version {VERSION}, build {BUILD}"
        print(identity, flush=True)
        self.log.write(identity + "\n")
        self.command([
            str(avdmanager), "create", "avd", "--force", "-n", selected.avd_name,
            "--package", image, "--device", selected.profile,
        ], input="no\n")
        config = avd_home / f"{selected.avd_name}.avd/config.ini"
        lines = [line for line in config.read_text().splitlines() if not line.startswith(("hw.cpu.ncore=", "disk.dataPartition.size="))]
        config.write_text("\n".join([*lines, "hw.cpu.ncore=2", "disk.dataPartition.size=6G"]) + "\n")

    def adb(self, command: list[str], timeout: float = 180) -> str:
        return self.command([
            str(self.selected.sdk_root / "platform-tools/adb"), "-L", ADB_ENDPOINT, "-s", SERIAL, *command,
        ], timeout=timeout)

    def check_adb_server(self) -> None:
        if self.adb_server is not None and self.adb_server.poll() is not None:
            raise RunnerError("The owned ADB server exited")

    def start_adb_server(self) -> None:
        self.adb_server_log = (self.selected.log_dir / "adb-server.log").open("ab")
        self.adb_server = subprocess.Popen([
            str(self.selected.sdk_root / "platform-tools/adb"), "-L", "tcp:5037", "server", "nodaemon",
        ], env=self.environment, stdout=self.adb_server_log, stderr=subprocess.STDOUT, start_new_session=True)
        deadline = time.monotonic() + 15
        while True:
            self.check_adb_server()
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise RunnerError("The owned ADB server startup deadline expired")
            try:
                with socket.create_connection(("127.0.0.1", 5037), timeout=min(0.1, remaining)):
                    self.check_adb_server()
                    return
            except OSError:
                time.sleep(min(0.1, max(0, deadline - time.monotonic())))

    def execute(self) -> int:
        check_ports()
        self.start_adb_server()
        options = ["-no-snapshot-save", "-no-window"]
        if self.selected.memory:
            options.extend(("-memory", self.selected.memory))
        options.extend(("-gpu", "swiftshader_indirect", "-noaudio", "-no-boot-anim", "-camera-back", "none"))
        with (self.selected.log_dir / "emulator.log").open("ab") as emulator_log:
            self.emulator = subprocess.Popen([
                str(self.selected.sdk_root / "emulator/emulator"), "-port", "5554",
                "-avd", self.selected.avd_name, *options,
            ], env=self.environment, stdout=emulator_log, stderr=subprocess.STDOUT, start_new_session=True)
            deadline = time.monotonic() + 900
            if self.emulator.poll() is not None:
                raise RunnerError("The emulator exited before the device wait")
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise RunnerError("The emulator boot deadline expired")
            self.adb(["wait-for-device"], min(180, remaining))
            while True:
                if self.emulator.poll() is not None:
                    raise RunnerError("The emulator exited before boot completed")
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise RunnerError("The emulator boot deadline expired")
                if self.adb(["shell", "getprop", "sys.boot_completed"], min(180, remaining)).strip() == "1":
                    break
                time.sleep(min(2, max(0, deadline - time.monotonic())))
            self.adb(["shell", "input", "keyevent", "82"])
            for setting in ("window_animation_scale", "transition_animation_scale", "animator_duration_scale"):
                self.adb(["shell", "settings", "put", "global", setting, "0.0"])
            if self.emulator.poll() is not None:
                raise RunnerError("The emulator exited before the test command")
            self.check_adb_server()
            self.test = subprocess.Popen(self.selected.command, env=self.environment, start_new_session=True)
            while True:
                result = self.test.poll()
                if result is not None:
                    self.test_status = result if result >= 0 else 128 - result
                if self.emulator.poll() is not None:
                    raise RunnerError("The emulator exited while the test command ran")
                self.check_adb_server()
                if self.test_status is not None:
                    return self.test_status
                time.sleep(0.1)

    def run(self) -> int:
        previous = {number: signal.getsignal(number) for number in (signal.SIGINT, signal.SIGTERM)}
        for number in previous:
            signal.signal(number, self.cancel)
        status = 1
        try:
            self.prepare()
            if self.cleanup_failed:
                raise RunnerError("An owned preparation command did not stop")
            status = self.execute()
        except Cancelled:
            status = 128 + self.cancel_signal
        except (RunnerError, OSError, ValueError, ET.ParseError, zipfile.BadZipFile, subprocess.TimeoutExpired) as error:
            print(f"Android emulator runner failed: {error}", file=sys.stderr)
            if self.log:
                try:
                    self.log.write(f"Android emulator runner failed: {error}\n")
                except (OSError, ValueError):
                    pass
        finally:
            self.cleaning = True
            try:
                for process in (self.test, self.emulator, self.adb_server, self.command_process):
                    if process is None:
                        continue
                    try:
                        stop_group(process)
                    except (RunnerError, OSError, ValueError) as error:
                        print(f"Android emulator cleanup failed: {error}", file=sys.stderr)
                        if self.log:
                            try:
                                self.log.write(f"Android emulator cleanup failed: {error}\n")
                            except (OSError, ValueError):
                                pass
                        if status == 0:
                            status = 1
                if self.cleanup_failed and status == 0:
                    status = 1
                for name, log in (("Android emulator", self.log), ("ADB server", self.adb_server_log)):
                    if log is None:
                        continue
                    try:
                        log.close()
                    except (OSError, ValueError) as error:
                        self.cleanup_failed = True
                        print(f"{name} log cleanup failed: {error}", file=sys.stderr)
                        if status == 0:
                            status = 1
            finally:
                try:
                    for number, handler in previous.items():
                        signal.signal(number, handler)
                finally:
                    self.cleaning = False
        if self.test_status:
            return self.test_status
        if self.cancel_signal:
            return 128 + self.cancel_signal
        return status


def main(argv: list[str] | None = None) -> int:
    return Runner(arguments(argv)).run()


if __name__ == "__main__":
    raise SystemExit(main())
