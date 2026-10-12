from __future__ import annotations

import json
import os
from pathlib import Path
import shlex
import subprocess
import sys
import tempfile
import time
import unittest


REPO_ROOT = Path(__file__).resolve().parents[2]
SERIAL = "emulator-5554"
REQUIRED_COMMANDS = ("svc", "settings", "input", "wm", "am", "cmd", "dumpsys")
HOME_COMMAND = [
    "shell", "cmd", "package", "resolve-activity", "--brief",
    "-a", "android.intent.action.MAIN", "-c", "android.intent.category.HOME",
]
PREPARATION_COMMANDS = [
    ["shell", "svc", "power", "stayon", "true"],
    ["shell", "settings", "put", "system", "screen_off_timeout", "2147483647"],
    ["shell", "settings", "put", "global", "hide_error_dialogs", "1"],
    ["shell", "input", "keyevent", "224"],
    ["shell", "wm", "dismiss-keyguard"],
    ["shell", "am", "broadcast", "-a", "android.intent.action.CLOSE_SYSTEM_DIALOGS"],
    ["shell", "input", "keyevent", "3"],
]
ADB_FIXTURE = r"""
import json
import os
from pathlib import Path
import subprocess
import sys

arguments = sys.argv[1:]
with Path(os.environ["PREPARE_COMMAND_LOG"]).open("a") as log:
    log.write(json.dumps(arguments) + "\n")
if arguments[:2] != ["-L", "tcp:127.0.0.1:5037"]:
    print("Fixture requires the local Android server socket", file=sys.stderr)
    raise SystemExit(2)
configuration = json.loads(Path(os.environ["PREPARE_CONFIGURATION"]).read_text())
if arguments[2:] == ["devices", "-l"]:
    sys.stdout.write(configuration.get("devices", "List of devices attached\nemulator-5554 device product:sdk model:sdk device:generic transport_id:1\n\n"))
    raise SystemExit(0)
if arguments[2:4] != ["-s", os.environ["PREPARE_SERIAL"]]:
    print("Fixture requires the selected Android serial", file=sys.stderr)
    raise SystemExit(2)
command = arguments[4:]
if command == ["shell", "getprop", "ro.build.version.sdk"]:
    print(configuration.get("sdk", "24"))
    raise SystemExit(0)
if command == ["wait-for-device"]:
    raise SystemExit(0)
if len(command) == 2 and command[0] == "shell":
    result = subprocess.run(
        ["/bin/sh", "-c", command[1]],
        env={**os.environ, "PATH": os.environ["PREPARE_DEVICE_BIN"]},
    )
    raise SystemExit(result.returncode)
if command[:4] == ["shell", "cmd", "package", "resolve-activity"]:
    sys.stdout.write(configuration.get(
        "home_output", "priority=0\r\ncom.example.home/.Home\r\n"
    ))
    raise SystemExit(configuration.get("home_status", 0))
if command == ["shell", "dumpsys", "window"]:
    sys.stdout.write(configuration.get(
        "window_output", "mCurrentFocus=Window{fixture u0 com.example.home/.Home}\n"
    ))
    status = configuration.get("window_status", 0)
    if status:
        print("Fixture window query failed", file=sys.stderr)
    raise SystemExit(status)
if command == ["shell", "dumpsys", "power"]:
    print("mWakefulness=Awake\nmStayOn=true")
    raise SystemExit(configuration.get("power_status", 0))
allowed = [
    ["shell", "svc", "power", "stayon", "true"],
    ["shell", "settings", "put", "system", "screen_off_timeout", "2147483647"],
    ["shell", "settings", "put", "global", "hide_error_dialogs", "1"],
    ["shell", "input", "keyevent", "224"],
    ["shell", "wm", "dismiss-keyguard"],
    ["shell", "am", "broadcast", "-a", "android.intent.action.CLOSE_SYSTEM_DIALOGS"],
    ["shell", "input", "keyevent", "3"],
]
if command in allowed:
    if command == configuration.get("failed_command"):
        print("Fixture required preparation command failed", file=sys.stderr)
        raise SystemExit(9)
    raise SystemExit(0)
print("Fixture command is unavailable: " + " ".join(command), file=sys.stderr)
raise SystemExit(127)
"""

CHILD_MAKE_FIXTURE = r"""
import os
import subprocess
import sys
raise SystemExit(subprocess.run(["make", "--no-print-directory", "-f", os.environ["PREPARE_CHILD_MAKEFILE"], *sys.argv[1:]], timeout=10, close_fds=False).returncode)
"""

CHILD_LOG_FIXTURE = r"""
import json
import os
from pathlib import Path
import sys
import time
with Path(os.environ["PREPARE_CHILD_LOG"]).open("a") as stream:
    record = {"kind": sys.argv[1], "arguments": sys.argv[2:], "android": os.environ.get("ANDROID_SERIAL"), "kotlin": os.environ.get("KOTLIN_ANDROID_SERIAL"), "manifest": os.environ.get("PACKAGED_SMOKE_RELEASE_MANIFEST")}
    if os.environ.get("PREPARE_CHILD_MODE"):
        record["makeflags"] = os.environ.get("MAKEFLAGS", "")
    stream.write(json.dumps(record) + "\n")
if sys.argv[1:3] == ["child", "android-emulator-prepare"]:
    mode = os.environ.get("PREPARE_CHILD_MODE")
    if mode == "fail":
        raise SystemExit(9)
    if mode == "block":
        Path(os.environ["PREPARE_CHILD_STARTED"]).write_text("started")
        deadline = time.monotonic() + 8
        while not Path(os.environ["PREPARE_CHILD_RELEASE"]).exists():
            if time.monotonic() >= deadline:
                raise SystemExit(96)
            time.sleep(0.01)
        with Path(os.environ["PREPARE_CHILD_LOG"]).open("a") as stream:
            stream.write(json.dumps({"kind": "prepared"}) + "\n")
"""

PYTHON_FIXTURE = r"""
import os
import sys
if sys.argv[1:3] == ["verification/packaged_smoke.py", "begin-cell"]:
    raise SystemExit(0)
if sys.argv[1:3] != ["verification/probe_support_environment.py", "resolve-android-serial"]:
    raise SystemExit("Unexpected fixture Python command")
os.execv(sys.executable, [sys.executable, "-B", *sys.argv[1:]])
"""


class AndroidEmulatorPrepareTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.sdk = self.root / "sdk"
        platform_tools = self.sdk / "platform-tools"
        platform_tools.mkdir(parents=True)
        fixture_source = self.root / "adb_fixture.py"
        fixture_source.write_text(ADB_FIXTURE)
        adb = platform_tools / "adb"
        adb.write_text(
            "#!/bin/sh\nexec "
            + shlex.quote(sys.executable)
            + " -B "
            + shlex.quote(str(fixture_source))
            + ' "$@"\n'
        )
        adb.chmod(0o700)
        self.device_bin = self.root / "device-bin"
        self.device_bin.mkdir()
        for command in REQUIRED_COMMANDS:
            executable = self.device_bin / command
            executable.write_text("#!/bin/sh\nexit 0\n")
            executable.chmod(0o700)
        self.host_bin = self.root / "host-bin"
        self.host_bin.mkdir()
        sleep = self.host_bin / "sleep"
        sleep.write_text('#!/bin/sh\nprintf "%s\\n" "$*" >> "$PREPARE_SLEEP_LOG"\n')
        sleep.chmod(0o700)
        self.command_log = self.root / "commands.jsonl"
        self.sleep_log = self.root / "sleep.log"
        self.configuration = self.root / "configuration.json"
        self.environment = {
            **os.environ,
            "PREPARE_SERIAL": SERIAL,
            "PREPARE_COMMAND_LOG": str(self.command_log),
            "PREPARE_SLEEP_LOG": str(self.sleep_log),
            "PREPARE_CONFIGURATION": str(self.configuration),
            "PREPARE_DEVICE_BIN": str(self.device_bin),
            "ANDROID_SERIAL": SERIAL,
        }
        for variable in ("MAKEFLAGS", "MFLAGS", "MAKELEVEL", "KOTLIN_ANDROID_SERIAL"):
            self.environment.pop(variable, None)
        self.child_log = self.root / "child.jsonl"
        self.manifest = self.root / "manifest.json"
        self.manifest.write_text("{}")
        self.child_logger = self.root / "child_logger.py"
        self.child_logger.write_text(CHILD_LOG_FIXTURE)
        self.child_makefile = self.root / "child.mk"
        self.child_makefile.write_text(
            ".PHONY: test-consumer-kotlin android-emulator-prepare client-consumer-kotlin-artifact client-consumer-rn-artifact test-consumer-kotlin-device-smoke test-consumer-rn-android-smoke\n"
            "test-consumer-kotlin android-emulator-prepare client-consumer-kotlin-artifact client-consumer-rn-artifact test-consumer-kotlin-device-smoke test-consumer-rn-android-smoke:\n"
            "\t@" + shlex.quote(sys.executable) + " -B " + shlex.quote(str(self.child_logger))
            + ' child "$@" "$(ANDROID_SERIAL)" "$(KOTLIN_ANDROID_SERIAL)"\n'
        )
        self.child_make = self.root / "child_make"
        self.child_make.write_text("#!" + sys.executable + "\n" + CHILD_MAKE_FIXTURE)
        self.child_make.chmod(0o700)
        python = self.host_bin / "python3"
        python.write_text("#!" + sys.executable + "\n" + PYTHON_FIXTURE)
        python.chmod(0o700)
        consumer = self.host_bin / "sh"
        consumer.write_text(
            "#!/bin/sh\ncase \"$1\" in verification/consumers/kotlin/test-consumer-device.sh|verification/consumers/react-native/test-consumer.sh) ;; *) exit 97 ;; esac\nexec "
            + shlex.quote(sys.executable) + " -B " + shlex.quote(str(self.child_logger)) + ' consumer "$@"\n'
        )
        consumer.chmod(0o700)
        self.fixture_shell = self.root / "fixture-shell"
        self.fixture_shell.write_text(
            '#!/bin/sh\ntest() { if [ "$1" = -r ] && [ "$2" = "$PREPARE_UNREADABLE_MANIFEST" ]; then return 1; fi; command test "$@"; }\n'
            '[ "$1" = -c ] || exit 98\neval "$2"\n'
        )
        self.fixture_shell.chmod(0o700)
        self.environment.update({"PREPARE_CHILD_MAKEFILE": str(self.child_makefile), "PREPARE_CHILD_LOG": str(self.child_log), "PREPARE_UNREADABLE_MANIFEST": ""})

    def invoke(
        self, configuration: dict | None = None, *, no_sleep: bool = False,
        target: str = "android-emulator-prepare", kotlin_serial: str | None = SERIAL,
        variables: dict[str, str] | None = None,
    ) -> subprocess.CompletedProcess:
        self.configuration.write_text(json.dumps(configuration or {}))
        self.command_log.unlink(missing_ok=True)
        self.sleep_log.unlink(missing_ok=True)
        self.child_log.unlink(missing_ok=True)
        environment = self.environment.copy()
        if no_sleep:
            environment["PATH"] = str(self.host_bin) + os.pathsep + environment["PATH"]
        result = subprocess.run(
            [
                "make", "--no-print-directory", target,
                f"ANDROID_HOME={self.sdk}",
                *([] if kotlin_serial is None else [f"KOTLIN_ANDROID_SERIAL={kotlin_serial}"]),
                "ANDROID_JAVA_HOME=",
                *[f"{name}={value}" for name, value in (variables or {}).items()],
            ],
            cwd=REPO_ROOT,
            env=environment,
            capture_output=True,
            text=True,
            timeout=15,
        )
        self.commands = [
            json.loads(line) for line in (self.command_log.read_text().splitlines() if self.command_log.exists() else [])
        ]
        for command in self.commands:
            self.assertEqual(command[:2], ["-L", "tcp:127.0.0.1:5037"])
            if command[2:] != ["devices", "-l"]:
                self.assertEqual(command[2:4], ["-s", SERIAL])
        self.commands = [command[2:] if command[2:] == ["devices", "-l"] else command[4:] for command in self.commands]
        return result

    def children(self) -> list[dict]:
        return [json.loads(line) for line in self.child_log.read_text().splitlines()] if self.child_log.exists() else []

    def smoke_variables(self) -> dict[str, str]:
        return {"MAKE": str(self.child_make), "PACKAGED_SMOKE_RELEASE_MANIFEST": str(self.manifest), "PACKAGED_SMOKE_TMP_ROOT": str(self.root / "tmp"), "PACKAGED_SMOKE_CELL_ID": "SUP-ANDROID-MIN-001", "PACKAGED_SMOKE_CELL_RESULT": str(self.root / "cell.json")}

    def test_supported_preparation_without_locksettings(self) -> None:
        self.assertFalse((self.device_bin / "locksettings").exists())
        result = self.invoke()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.commands[0], ["devices", "-l"])
        self.assertEqual(self.commands[1], ["wait-for-device"])
        self.assertEqual(self.commands[2][0], "shell")
        self.assertEqual(len(self.commands[2]), 2)
        self.assertEqual(self.commands[3:], [
            *PREPARATION_COMMANDS, HOME_COMMAND, ["shell", "dumpsys", "window"]
        ])
        self.assertFalse(self.sleep_log.exists())

    def test_each_missing_required_command_stops_before_settings(self) -> None:
        for command in REQUIRED_COMMANDS:
            with self.subTest(command=command):
                executable = self.device_bin / command
                executable.unlink()
                try:
                    result = self.invoke()
                    self.assertNotEqual(result.returncode, 0, result.stdout)
                    self.assertIn("Required Android command missing", result.stderr)
                    self.assertIn(command, result.stderr)
                    self.assertEqual(len(self.commands), 3)
                    self.assertEqual(self.commands[0], ["devices", "-l"])
                    self.assertEqual(self.commands[1], ["wait-for-device"])
                    self.assertEqual(self.commands[2][0], "shell")
                finally:
                    executable.write_text("#!/bin/sh\nexit 0\n")
                    executable.chmod(0o700)

    def test_home_resolution_failure_rejects_valid_looking_output(self) -> None:
        result = self.invoke({"home_status": 7})
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertEqual(self.commands[-1], HOME_COMMAND)
        self.assertNotIn(["shell", "dumpsys", "window"], self.commands)

    def test_invalid_successful_home_resolution_stops_preparation(self) -> None:
        for output in (
            "", "No activity found\n", "/.Home\n", "com.example.home/\n",
            "/bad/component\n", "com.example.home//Home\n",
        ):
            with self.subTest(output=output):
                result = self.invoke({"home_output": output})
                self.assertNotEqual(result.returncode, 0, result.stdout)
                self.assertIn("package/component", result.stderr)
                self.assertIn("Check the device Home activity", result.stderr)
                self.assertEqual(self.commands[-1], HOME_COMMAND)

    def test_window_failure_rejects_matching_looking_focus(self) -> None:
        result = self.invoke({"window_status": 8})
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn("Fixture window query failed", result.stderr)
        self.assertEqual(self.commands.count(["shell", "dumpsys", "window"]), 1)
        self.assertEqual(self.commands[-1], ["shell", "dumpsys", "window"])

    def test_wrong_focus_exhausts_polling_and_reports_diagnostics(self) -> None:
        result = self.invoke({
            "window_output": "mCurrentFocus=Window{fixture u0 com.example.other/.Other}\n"
                             "mFocusedApp=AppWindowToken{fixture com.example.other/.Other}\n",
            # A diagnostic command failure must not replace the focus failure.
            "power_status": 6,
        }, no_sleep=True)
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn("Android Home did not receive window focus", result.stderr)
        self.assertIn("mWakefulness=Awake", result.stderr)
        self.assertIn("mStayOn=true", result.stderr)
        self.assertIn("mCurrentFocus=Window{fixture u0 com.example.other/.Other}", result.stderr)
        self.assertIn("mFocusedApp=AppWindowToken{fixture com.example.other/.Other}", result.stderr)
        self.assertEqual(self.commands.count(["shell", "dumpsys", "window"]), 31)
        self.assertEqual(self.commands[-2:], [
            ["shell", "dumpsys", "power"], ["shell", "dumpsys", "window"]
        ])
        self.assertEqual(self.sleep_log.read_text().splitlines(), ["1"] * 30)

    def test_required_preparation_command_failure_stops_the_chain(self) -> None:
        for command in PREPARATION_COMMANDS:
            with self.subTest(command=command):
                result = self.invoke({"failed_command": command})
                self.assertNotEqual(result.returncode, 0, result.stdout)
                self.assertIn("Fixture required preparation command failed", result.stderr)
                self.assertEqual(self.commands[-1], command)
                self.assertNotIn(HOME_COMMAND, self.commands)

    def test_original_conflicts_and_whitespace_are_rejected_before_commands(self) -> None:
        for inherited in ("emulator-5556", SERIAL + " ", " " + SERIAL):
            with self.subTest(inherited=inherited):
                self.environment["ANDROID_SERIAL"] = inherited
                result = self.invoke()
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("Original ANDROID_SERIAL and KOTLIN_ANDROID_SERIAL values disagree", result.stderr)
                self.assertEqual(self.commands, [])

    def test_only_one_original_value_and_unique_resolution(self) -> None:
        result = self.invoke(kotlin_serial=None)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.environment.pop("ANDROID_SERIAL")
        result = self.invoke()
        self.assertEqual(result.returncode, 0, result.stderr)
        result = self.invoke(kotlin_serial=None)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.commands[0], ["devices", "-l"])

    def test_matching_malformed_original_serials_stop_at_resolver(self) -> None:
        for serial in (SERIAL + " ", "emulator-05554", "emulator-65536"):
            with self.subTest(serial=serial):
                self.environment["ANDROID_SERIAL"] = serial
                result = self.invoke(kotlin_serial=serial)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("environment probe failed", result.stderr)
                self.assertEqual(self.commands, [])

    def test_missing_offline_and_duplicate_devices_stop_before_preparation(self) -> None:
        for devices in ("List of devices attached\n", "List of devices attached\nemulator-5554 offline\n", "List of devices attached\nemulator-5554 device\nemulator-5554 device\n"):
            with self.subTest(devices=devices):
                result = self.invoke({"devices": devices})
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(self.commands, [["devices", "-l"]])

    def test_explicit_device_coexists_with_other_devices(self) -> None:
        result = self.invoke({"devices": "List of devices attached\nemulator-5554 device product:sdk\nemulator-5556 device product:other\nphysical-device device\n"})
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.commands[-1], ["shell", "dumpsys", "window"])

    def test_hostile_server_endpoints_keep_all_commands_local(self) -> None:
        endpoints = {"ADB_SERVER_SOCKET": "tcp:remote.example:9999", "ANDROID_ADB_SERVER_ADDRESS": "remote.example", "ANDROID_ADB_SERVER_PORT": "9999"}
        for inherited in (*({name: value} for name, value in endpoints.items()), endpoints):
            with self.subTest(inherited=inherited):
                self.environment.update(inherited)
                result = self.invoke()
                self.assertEqual(result.returncode, 0, result.stderr)
                for name in inherited:
                    self.environment.pop(name)

    def test_manifest_rejection_precedes_smoke_builds_and_devices(self) -> None:
        for target in ("test-consumer-kotlin-device-smoke", "test-consumer-rn-android-smoke"):
            for manifest in ("", str(self.root / "missing.json"), str(self.root), str(self.manifest)):
                with self.subTest(target=target, manifest=manifest):
                    variables = self.smoke_variables()
                    variables["PACKAGED_SMOKE_RELEASE_MANIFEST"] = manifest
                    if manifest == str(self.manifest):
                        self.environment["PREPARE_UNREADABLE_MANIFEST"] = manifest
                        variables["SHELL"] = str(self.fixture_shell)
                    result = self.invoke(target=target, variables=variables, no_sleep=True)
                    self.environment["PREPARE_UNREADABLE_MANIFEST"] = ""
                    self.assertNotEqual(result.returncode, 0)
                    self.assertIn("PACKAGED_SMOKE_RELEASE_MANIFEST must be an explicit readable file", result.stderr)
                    self.assertEqual(self.commands, [])
                    self.assertEqual(self.children(), [])

    def test_ordered_smoke_goals_manifest_and_empty_command_line_serials(self) -> None:
        self.environment.pop("ANDROID_SERIAL")
        for target, goals in (("test-consumer-kotlin-device-smoke", ["test-consumer-kotlin"]), ("test-consumer-rn-android-smoke", ["android-emulator-prepare", "client-consumer-kotlin-artifact", "client-consumer-rn-artifact"])):
            with self.subTest(target=target):
                variables = {**self.smoke_variables(), "ANDROID_SERIAL": ""}
                result = self.invoke(target=target, kotlin_serial="", variables=variables, no_sleep=True)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(self.commands, [["devices", "-l"]])
                records = self.children()
                self.assertEqual([record["arguments"][0] for record in records[:-1]], goals)
                self.assertEqual(records[-1]["kind"], "consumer")
                self.assertEqual(records[-1]["manifest"], str(self.manifest))
                for record in records:
                    self.assertEqual(record["android"], SERIAL)
                    self.assertEqual(record["kotlin"], SERIAL)
                    if record["kind"] == "child":
                        self.assertEqual(record["arguments"][1:], [SERIAL, SERIAL])

    def test_both_platform_sdk_queries_and_recursive_selection(self) -> None:
        self.environment.pop("ANDROID_SERIAL")
        for cell, sdk, goal in (("SUP-ANDROID-MIN-001", "24", "test-consumer-kotlin-device-smoke"), ("SUP-ANDROID-CURRENT-001", "37", "test-consumer-kotlin-device-smoke"), ("SUP-RN-ANDROID-CURRENT-001", "37", "test-consumer-rn-android-smoke")):
            with self.subTest(cell=cell):
                variables = {**self.smoke_variables(), "ANDROID_SERIAL": "", "SUPPORT_CELL_ID": cell, "SUPPORT_PLATFORM_VERSION": sdk, "PACKAGED_SMOKE_CELL_DIR": str(self.root / "cells"), "WARM_CONNECT_ENV": ""}
                result = self.invoke({"sdk": sdk}, target="test-client-platforms", kotlin_serial="", variables=variables, no_sleep=True)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(self.commands, [["devices", "-l"], ["shell", "getprop", "ro.build.version.sdk"]])
                self.assertEqual(self.children(), [{"kind": "child", "arguments": [goal, SERIAL, SERIAL], "android": SERIAL, "kotlin": SERIAL, "manifest": str(self.manifest)}])
                mismatch = self.invoke({"sdk": "25"}, target="test-client-platforms", kotlin_serial="", variables=variables, no_sleep=True)
                self.assertNotEqual(mismatch.returncode, 0)
                self.assertEqual(self.children(), [])

    def test_inherited_parallel_make_cannot_start_artifacts_before_preparation(self) -> None:
        self.configuration.write_text("{}")
        started, release = self.root / "started", self.root / "release"
        environment = {**self.environment, "PATH": str(self.host_bin) + os.pathsep + self.environment["PATH"], "MAKEFLAGS": "-j4", "PREPARE_CHILD_MODE": "block", "PREPARE_CHILD_STARTED": str(started), "PREPARE_CHILD_RELEASE": str(release)}
        process = subprocess.Popen(
            ["make", "--no-print-directory", "test-consumer-rn-android-smoke", f"ANDROID_HOME={self.sdk}", f"KOTLIN_ANDROID_SERIAL={SERIAL}", "ANDROID_JAVA_HOME=", *[f"{name}={value}" for name, value in self.smoke_variables().items()]],
            cwd=REPO_ROOT, env=environment, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
        try:
            deadline = time.monotonic() + 5
            while not started.exists() and process.poll() is None and time.monotonic() < deadline:
                time.sleep(0.01)
            self.assertTrue(started.exists())
            time.sleep(0.2)
            self.assertIsNone(process.poll())
            records = self.children()
            self.assertEqual(len(records), 1)
            self.assertEqual(records[0]["arguments"][0], "android-emulator-prepare")
            self.assertRegex(records[0]["makeflags"], r"(?:^| )-j(?:4)?(?: |$)")
            self.assertIn("--jobserver-", records[0]["makeflags"])
        finally:
            release.write_text("release")
            stdout, stderr = process.communicate(timeout=12)
        self.assertEqual(process.returncode, 0, stderr)
        records = self.children()
        self.assertEqual([record["kind"] for record in records], ["child", "prepared", "child", "child", "consumer"])
        self.assertEqual([record["arguments"][0] for record in records[2:4]], ["client-consumer-kotlin-artifact", "client-consumer-rn-artifact"])
        self.assertEqual(records[-1]["manifest"], str(self.manifest))

    def test_preparation_failure_with_inherited_parallel_flags_stops_artifacts_and_consumer(self) -> None:
        self.environment.update({"MAKEFLAGS": "-j4", "PREPARE_CHILD_MODE": "fail"})
        result = self.invoke(target="test-consumer-rn-android-smoke", variables=self.smoke_variables(), no_sleep=True)
        self.assertNotEqual(result.returncode, 0)
        records = self.children()
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["kind"], "child")
        self.assertEqual(records[0]["arguments"], ["android-emulator-prepare", SERIAL, SERIAL])
        self.assertRegex(records[0]["makeflags"], r"(?:^| )-j(?:4)?(?: |$)")
        self.assertIn("--jobserver-", records[0]["makeflags"])
        self.assertEqual(self.commands, [["devices", "-l"]])


if __name__ == "__main__":
    unittest.main()
