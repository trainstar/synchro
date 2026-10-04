from __future__ import annotations

import json
import os
from pathlib import Path
import shlex
import subprocess
import sys
import tempfile
import unittest


REPO_ROOT = Path(__file__).resolve().parents[2]
SERIAL = "fixture-android"
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
if arguments[:2] != ["-s", os.environ["PREPARE_SERIAL"]]:
    print("Fixture requires the selected Android serial", file=sys.stderr)
    raise SystemExit(2)
command = arguments[2:]
configuration = json.loads(Path(os.environ["PREPARE_CONFIGURATION"]).read_text())
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
            # The Makefile must override this inherited serial with its selector.
            "ANDROID_SERIAL": "unselected-fixture",
        }
        for variable in ("MAKEFLAGS", "MFLAGS", "MAKELEVEL"):
            self.environment.pop(variable, None)

    def invoke(
        self, configuration: dict | None = None, *, no_sleep: bool = False
    ) -> subprocess.CompletedProcess:
        self.configuration.write_text(json.dumps(configuration or {}))
        self.command_log.unlink(missing_ok=True)
        self.sleep_log.unlink(missing_ok=True)
        environment = self.environment.copy()
        if no_sleep:
            environment["PATH"] = str(self.host_bin) + os.pathsep + environment["PATH"]
        result = subprocess.run(
            [
                "make", "--no-print-directory", "android-emulator-prepare",
                f"ANDROID_HOME={self.sdk}", f"KOTLIN_ANDROID_SERIAL={SERIAL}",
                "ANDROID_JAVA_HOME=",
            ],
            cwd=REPO_ROOT,
            env=environment,
            capture_output=True,
            text=True,
            timeout=15,
        )
        self.commands = [
            json.loads(line) for line in self.command_log.read_text().splitlines()
        ]
        for command in self.commands:
            self.assertEqual(command[:2], ["-s", SERIAL])
        self.commands = [command[2:] for command in self.commands]
        return result

    def test_supported_preparation_without_locksettings(self) -> None:
        self.assertFalse((self.device_bin / "locksettings").exists())
        result = self.invoke()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.commands[0], ["wait-for-device"])
        self.assertEqual(self.commands[1][0], "shell")
        self.assertEqual(len(self.commands[1]), 2)
        self.assertEqual(self.commands[2:], [
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
                    self.assertEqual(len(self.commands), 2)
                    self.assertEqual(self.commands[0], ["wait-for-device"])
                    self.assertEqual(self.commands[1][0], "shell")
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


if __name__ == "__main__":
    unittest.main()
