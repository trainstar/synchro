from __future__ import annotations

import contextlib
import io
import json
import os
from pathlib import Path
import re
import signal
import stat
import subprocess
import sys
import tempfile
import textwrap
import types
import unittest
from unittest import mock
import zipfile

from scripts.ci import run_android_emulator as runner


USER_PROCESS_STAT = "1234 (fixture command with ) parentheses) S 1 1234 1234 0 -1 4194304\n"


def archive_member(name: str, data: bytes = b"", mode: int = stat.S_IFREG | 0o644) -> tuple:
    member = zipfile.ZipInfo(name)
    member.create_system = 3
    member.external_attr = mode << 16
    return member, data


class Process:
    def __init__(self, pid: int, output: str = "", status: int = 0, polls: list | None = None):
        self.pid = pid
        self.output = output
        self.returncode = None
        self.status = status
        self.polls = list(polls) if polls is not None else [status]
        self.communications = []

    def communicate(self, input=None, timeout=None):
        self.communications.append((input, timeout))
        self.returncode = self.status
        return self.output, None

    def poll(self):
        result = self.polls.pop(0) if len(self.polls) > 1 else self.polls[0]
        self.returncode = result
        return result

    def wait(self, timeout=None):
        self.returncode = self.status
        return self.status


class EmulatorRunnerTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.sdk = self.root / "sdk"
        self.manager = self.sdk / "cmdline-tools/latest/bin/sdkmanager"
        for path in (self.manager, self.manager.with_name("avdmanager"), self.sdk / "platform-tools/adb"):
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("fixture")
            path.chmod(0o755)
        self.argv = [
            "--sdk-root", str(self.sdk), "--sdkmanager", str(self.manager),
            "--api", "34", "--image-api", "34", "--avd-name", "Synchro_Kotlin_API_34",
            "--profile", "pixel_2", "--log-dir", str(self.root / "logs"),
            "--", "make", "ci-candidate-kotlin",
        ]
        self.calls = []
        self.stopped = []
        self.replacement = None
        self.boot = "1\n"
        self.emulator_polls = [None]
        self.test_polls = [0]
        self.version = "Android emulator version 37.2.12.0 (build_id 16428233) (CL:N/A)\n"
        self.properties = b"Pkg.Revision=37.2.12\nPkg.BuildId=16428233\nPkg.Path=emulator\n"
        self.source_archive = self.root / "fixture.zip"
        self.write_archive([
            archive_member("emulator/", mode=stat.S_IFDIR | 0o755),
            archive_member("emulator/emulator", b"fixture executable", stat.S_IFREG | 0o755),
            archive_member("emulator/source.properties", self.properties),
            archive_member("emulator/lib/", mode=stat.S_IFDIR | 0o755),
            archive_member("emulator/lib/data", b"fixture library"),
        ])

    def write_archive(self, members):
        with zipfile.ZipFile(self.source_archive, "w") as archive:
            for member, data in members:
                archive.writestr(member, data)

    def selected(self):
        return runner.arguments(self.argv)

    def popen(self, command, **options):
        self.calls.append((list(command), options))
        name = Path(command[0]).name
        output = ""
        polls = None
        if name == "curl":
            Path(command[command.index("--output") + 1]).write_bytes(self.source_archive.read_bytes())
        elif name == "sdkmanager":
            if self.replacement:
                self.replacement()
        elif name == "sudo":
            output = "/usr/bin/fixture\n"
        elif name == "emulator":
            if "-version" in command:
                output = self.version
            else:
                polls = self.emulator_polls
        elif name == "avdmanager":
            config = Path(options["env"]["ANDROID_AVD_HOME"]) / f"{command[command.index('-n') + 1]}.avd/config.ini"
            config.parent.mkdir(exist_ok=True)
            config.write_text("hw.cpu.ncore=8\nhw.ramSize=2048\n")
        elif name == "adb":
            if command[-2:] == ["getprop", "sys.boot_completed"]:
                output = self.boot
        elif name == "make":
            polls = self.test_polls
        else:
            self.fail(f"Unexpected fixture command: {command}")
        return Process(1000 + len(self.calls), output, polls=polls)

    @contextlib.contextmanager
    def boundaries(self, *, verified_archive=True, environment=None):
        real_stat = Path.stat
        real_digest = runner.digest

        def measured_stat(path, *args, **kwargs):
            result = real_stat(path, *args, **kwargs)
            if path.name == "emulator.zip" and verified_archive:
                return types.SimpleNamespace(st_size=runner.ARCHIVE_SIZE, st_mode=result.st_mode)
            return result

        def measured_digest(path):
            if path.name == "emulator.zip" and verified_archive:
                return runner.ARCHIVE_SHA256
            return real_digest(path)

        with contextlib.ExitStack() as stack:
            stack.enter_context(mock.patch.dict(os.environ, environment or {}, clear=True))
            stack.enter_context(mock.patch.object(runner.platform, "system", return_value="Linux"))
            stack.enter_context(mock.patch.object(runner.platform, "machine", return_value="x86_64"))
            stack.enter_context(mock.patch.object(runner, "check_ports"))
            stack.enter_context(mock.patch.object(runner, "check_active_emulator"))
            stack.enter_context(mock.patch.object(runner, "stop_group", side_effect=lambda process: self.stopped.append(process.pid)))
            stack.enter_context(mock.patch.object(runner.subprocess, "Popen", side_effect=self.popen))
            stack.enter_context(mock.patch.object(runner.time, "sleep"))
            stack.enter_context(mock.patch.object(Path, "stat", measured_stat))
            stack.enter_context(mock.patch.object(runner, "digest", side_effect=measured_digest))
            stack.enter_context(contextlib.redirect_stdout(io.StringIO()))
            stack.enter_context(contextlib.redirect_stderr(io.StringIO()))
            yield

    def run_fixture(self, **options):
        with self.boundaries(**options):
            return runner.Runner(self.selected()).run()

    def commands(self, name):
        return [(command, options) for command, options in self.calls if Path(command[0]).name == name]

    def test_complete_selected_command_and_environment(self):
        environment = {
            "PATH": "/fixture/bin", "ANDROID_SERIAL": runner.SERIAL,
            "KOTLIN_ANDROID_SERIAL": runner.SERIAL, "ADB_SERVER_SOCKET": "tcp:hostile:7000",
            "ANDROID_ADB_SERVER_ADDRESS": "hostile", "ANDROID_ADB_SERVER_PORT": "7000",
        }
        self.assertEqual(self.run_fixture(environment=environment), 0)
        curl = self.commands("curl")[0][0]
        self.assertEqual(curl[:12], [
            "curl", "--fail", "--location", "--proto", "=https", "--proto-redir", "=https",
            "--connect-timeout", "30", "--max-time", "180", "--output",
        ])
        self.assertEqual(curl[-1], runner.ARCHIVE_URL)
        self.assertEqual(self.commands("sdkmanager")[0][0], [
            str(self.manager), f"--sdk_root={self.sdk}", "platforms;android-34",
            "system-images;android-34;google_apis;x86_64",
        ])
        avd, options = self.commands("avdmanager")[0]
        self.assertEqual(avd, [
            str(self.manager.with_name("avdmanager")), "create", "avd", "--force", "-n", "Synchro_Kotlin_API_34",
            "--package", "system-images;android-34;google_apis;x86_64", "--device", "pixel_2",
        ])
        self.assertEqual((self.root / "logs/avd/Synchro_Kotlin_API_34.avd/config.ini").read_text(), "hw.ramSize=2048\nhw.cpu.ncore=2\n")
        launch = self.commands("emulator")[1][0]
        self.assertEqual(launch, [
            str(self.sdk / "emulator/emulator"), "-port", "5554", "-avd", "Synchro_Kotlin_API_34",
            "-no-snapshot-save", "-no-window", "-gpu", "swiftshader_indirect", "-noaudio",
            "-no-boot-anim", "-camera-back", "none",
        ])
        test, options = self.commands("make")[0]
        self.assertEqual(test, ["make", "ci-candidate-kotlin"])
        self.assertTrue(options["start_new_session"])
        expected = options["env"]
        self.assertEqual(expected["ANDROID_HOME"], str(self.sdk))
        self.assertEqual(expected["ANDROID_SDK_ROOT"], str(self.sdk))
        self.assertEqual(expected["ANDROID_SERIAL"], runner.SERIAL)
        self.assertEqual(expected["KOTLIN_ANDROID_SERIAL"], runner.SERIAL)
        self.assertEqual(expected["ANDROID_AVD_HOME"], str(self.root / "logs/avd"))
        self.assertEqual(expected["ADB_SERVER_SOCKET"], runner.ADB_ENDPOINT)
        self.assertNotIn("ANDROID_ADB_SERVER_ADDRESS", expected)
        self.assertNotIn("ANDROID_ADB_SERVER_PORT", expected)
        self.assertEqual(expected["PATH"], f"{self.sdk}/platform-tools:{self.manager.parent}:/fixture/bin")
        for command, options in self.calls:
            self.assertTrue(options["start_new_session"])
            self.assertEqual(options["env"], expected)
        adb = [command for command, _ in self.commands("adb")]
        self.assertTrue(all(command[:5] == [str(self.sdk / "platform-tools/adb"), "-L", runner.ADB_ENDPOINT, "-s", runner.SERIAL] for command in adb))
        self.assertEqual(adb[-4:], [
            [*adb[0][:5], "shell", "input", "keyevent", "82"],
            *[[*adb[0][:5], "shell", "settings", "put", "global", name, "0.0"] for name in
              ("window_animation_scale", "transition_animation_scale", "animator_duration_scale")],
        ])
        self.assertEqual((self.sdk / "emulator/package.xml").read_bytes(), runner.METADATA.read_bytes())
        self.assertEqual(len(self.stopped), len(self.calls))
        self.assertIn(runner.ARCHIVE_SHA256, (self.root / "logs/runner.log").read_text())
        self.assertTrue((self.root / "logs/emulator.log").exists())
        self.assertEqual(list(self.sdk.glob(".synchro-emulator-*")), [])

    def test_memory_option_retains_react_native_position(self):
        self.argv[-3:-3] = ["--memory", "6144"]
        self.assertEqual(self.run_fixture(), 0)
        command = self.commands("emulator")[1][0]
        self.assertEqual(command[7:11], ["-memory", "6144", "-gpu", "swiftshader_indirect"])

    def test_all_selected_platform_and_image_pairs(self):
        for api, image in (("24", "24"), ("34", "34"), ("37", "37.0")):
            with self.subTest(api=api):
                argv = list(self.argv)
                argv[5] = api
                argv[7] = image
                argv[9] = f"Synchro_Release_API_{image}"
                selected = runner.arguments(argv)
                self.assertEqual((selected.api, selected.image_api), (api, image))

    def test_malformed_selections_are_rejected(self):
        cases = [("--api", "36"), ("--image-api", "37"), ("--avd-name", "../escape"), ("--profile", "unknown")]
        for flag, value in cases:
            with self.subTest(flag=flag), contextlib.redirect_stderr(io.StringIO()):
                argv = list(self.argv)
                argv[argv.index(flag) + 1] = value
                with self.assertRaises(SystemExit):
                    runner.arguments(argv)
        with contextlib.redirect_stderr(io.StringIO()):
            for suffix in (["--memory", "2048", "--", "make"], ["--memory", "6144"], ["make"]):
                with self.assertRaises(SystemExit):
                    runner.arguments(self.argv[:-3] + suffix)
            mismatched = list(self.argv)
            mismatched[7] = "24"
            with self.assertRaises(SystemExit):
                runner.arguments(mismatched)
        self.assertEqual(self.calls, [])

    def test_conflicting_serials_fail_before_sdk_changes(self):
        for variable in ("ANDROID_SERIAL", "KOTLIN_ANDROID_SERIAL"):
            with self.subTest(variable=variable):
                self.assertEqual(self.run_fixture(environment={variable: "other-device"}), 1)
                self.assertEqual(self.calls, [])
                self.assertFalse((self.sdk / "emulator").exists())

    def test_unsupported_host_fails_before_sdk_changes(self):
        with self.boundaries(), mock.patch.object(runner.platform, "machine", return_value="arm64"):
            self.assertEqual(runner.Runner(self.selected()).run(), 1)
        self.assertEqual(self.calls, [])

    def test_unreadable_tool_fails_before_sdk_changes(self):
        self.manager.with_name("avdmanager").unlink()
        self.assertEqual(self.run_fixture(), 1)
        self.assertEqual(self.calls, [])

    def test_malformed_and_changed_metadata_fail_before_sdk_changes(self):
        for metadata in (b"<invalid", runner.METADATA.read_bytes() + b"\n"):
            path = self.root / "metadata.xml"
            path.write_bytes(metadata)
            with self.subTest(metadata=metadata[:8]), mock.patch.object(runner, "METADATA", path):
                self.assertEqual(self.run_fixture(), 1)
                self.assertEqual(self.calls, [])

    def test_fixed_metadata_identity(self):
        self.assertEqual(runner.digest(runner.METADATA), runner.METADATA_SHA256)
        self.assertTrue(runner.METADATA.read_bytes().endswith(b"\n"))

    def test_occupied_port_fails_before_download(self):
        with self.boundaries(), mock.patch.object(runner, "check_ports", side_effect=runner.RunnerError("occupied")):
            self.assertEqual(runner.Runner(self.selected()).run(), 1)
        self.assertEqual(self.calls, [])

    def test_active_emulator_fails_before_download(self):
        with self.boundaries(), mock.patch.object(runner, "check_active_emulator", side_effect=runner.RunnerError("active")):
            self.assertEqual(runner.Runner(self.selected()).run(), 1)
        self.assertEqual(self.calls, [])

    def test_archive_size_rejection_preserves_previous_emulator(self):
        previous = self.sdk / "emulator"
        previous.mkdir()
        (previous / "retained").write_text("unchanged")
        self.assertEqual(self.run_fixture(verified_archive=False), 1)
        self.assertEqual((previous / "retained").read_text(), "unchanged")
        self.assertEqual(len(self.calls), 1)
        self.assertEqual(self.commands("make"), [])

    def test_archive_digest_rejection_prevents_installation(self):
        with self.boundaries(), mock.patch.object(runner, "digest", return_value="wrong"):
            self.assertEqual(runner.Runner(self.selected()).run(), 1)
        self.assertFalse((self.sdk / "emulator").exists())
        self.assertEqual(len(self.calls), 1)

    def test_unsafe_archive_members_prevent_installation(self):
        cases = [
            archive_member("/emulator/file"), archive_member("emulator/../file"),
            archive_member("outside/file"), archive_member("emulator//file"),
            archive_member("emulator/./file"), archive_member("emulator\\file"),
            archive_member("emulator/link", b"target", stat.S_IFLNK | 0o777),
            archive_member("emulator/pipe", mode=stat.S_IFIFO | 0o644),
            archive_member("emulator/file", mode=0o644),
            archive_member("emulator/file", mode=stat.S_IFREG | 0o4644),
            archive_member("emulator/package.xml"),
        ]
        for member in cases:
            with self.subTest(member=member[0].filename):
                self.write_archive([member])
                self.calls.clear()
                self.assertEqual(self.run_fixture(), 1)
                self.assertFalse((self.sdk / "emulator").exists())
                self.assertEqual(len(self.calls), 1)

    def test_duplicate_archive_paths_and_file_parents(self):
        cases = [
            [archive_member("emulator/file"), archive_member("emulator/file")],
            [archive_member("emulator/lib"), archive_member("emulator/lib/file")],
        ]
        for members in cases:
            with self.subTest(names=[member.filename for member, _ in members]):
                with contextlib.redirect_stderr(io.StringIO()):
                    self.write_archive(members)
                with self.assertRaises(runner.RunnerError):
                    runner.extract_archive(self.source_archive, self.root / "extracted")
                self.assertFalse((self.root / "extracted").exists())

    def test_sdk_replacement_bytes_modes_and_metadata_prevent_startup(self):
        changes = (
            lambda: (self.sdk / "emulator/emulator").write_bytes(b"replacement"),
            lambda: (self.sdk / "emulator/emulator").chmod(0o644),
            lambda: (self.sdk / "emulator/package.xml").write_bytes(b"replacement"),
            lambda: (self.sdk / "emulator/lib/data").unlink(),
        )
        for change in changes:
            with self.subTest(change=change):
                self.calls.clear()
                self.replacement = change
                self.assertEqual(self.run_fixture(), 1)
                self.assertEqual(self.commands("emulator"), [])
                self.assertEqual(self.commands("make"), [])
                self.assertEqual(len(self.commands("sdkmanager")), 1)

    def test_archive_properties_and_executable_version_must_match_pin(self):
        self.version = "Android emulator version 37.2.12.0 (build_id 16428234)\n"
        self.assertEqual(self.run_fixture(), 1)
        self.assertEqual(self.commands("make"), [])
        self.assertEqual(self.commands("avdmanager"), [])
        (self.sdk / "emulator/source.properties").write_text("Pkg.Revision=37.2.12\nPkg.BuildId=wrong\n")
        with self.assertRaises(runner.RunnerError):
            runner.check_properties(self.sdk / "emulator")

    def test_boot_deadline_is_monotonic_and_bounded(self):
        with self.boundaries():
            execution = runner.Runner(self.selected())
            execution.prepare()
            execution.adb = mock.Mock(return_value="0")
            with mock.patch.object(runner.time, "monotonic", side_effect=[0, 898, 899, 900, 900]):
                with self.assertRaises(runner.RunnerError):
                    execution.execute()
            self.assertEqual(execution.adb.call_args_list, [mock.call(["wait-for-device"], 2), mock.call(["shell", "getprop", "sys.boot_completed"], 1)])
            self.assertIsNone(execution.test)
            execution.log.close()

    def test_boot_early_exit_prevents_test_start(self):
        self.emulator_polls = [7]
        self.assertEqual(self.run_fixture(), 1)
        self.assertEqual(self.commands("make"), [])
        self.assertEqual(self.commands("adb"), [])

    def test_early_exit_after_boot_prevents_test_start(self):
        self.emulator_polls = [None, None, 7]
        self.assertEqual(self.run_fixture(), 1)
        self.assertEqual(self.commands("make"), [])

    def test_emulator_exit_during_test_is_visible(self):
        self.emulator_polls = [None, None, None, 7]
        self.test_polls = [None]
        self.assertEqual(self.run_fixture(), 1)
        self.assertEqual(len(self.commands("make")), 1)
        self.assertEqual(len(self.stopped), len(self.calls))

    def test_test_failure_and_signal_status_are_retained(self):
        for result, expected in ((9, 9), (-signal.SIGKILL, 137)):
            with self.subTest(result=result):
                self.calls.clear()
                self.test_polls = [result]
                self.assertEqual(self.run_fixture(), expected)

    def test_simultaneous_emulator_exit_retains_recorded_test_failure(self):
        self.emulator_polls = [None, None, None, 7]
        self.test_polls = [23]
        self.assertEqual(self.run_fixture(), 23)

    def test_cleanup_failure_is_visible_without_hiding_test_failure(self):
        for test_status, expected in ((0, 1), (9, 9)):
            with self.subTest(test_status=test_status), self.boundaries():
                execution = runner.Runner(self.selected())
                execution.test = Process(101)
                execution.emulator = Process(102)
                execution.command_process = Process(103)
                execution.test_status = test_status
                with mock.patch.object(execution, "prepare"), mock.patch.object(execution, "execute", return_value=test_status), mock.patch.object(runner, "stop_group", side_effect=runner.RunnerError("cleanup failed")) as cleanup:
                    self.assertEqual(execution.run(), expected)
                    self.assertEqual(cleanup.call_count, 3)

    def test_preparation_cleanup_failure_prevents_test_start(self):
        with self.boundaries():
            execution = runner.Runner(self.selected())
            def failed_cleanup():
                execution.cleanup_failed = True
            with mock.patch.object(execution, "prepare", side_effect=failed_cleanup), mock.patch.object(execution, "execute") as execute:
                self.assertEqual(execution.run(), 1)
                execute.assert_not_called()

    def test_command_cleanup_failure_keeps_original_failure_visible(self):
        with self.boundaries(), contextlib.redirect_stderr(io.StringIO()) as errors:
            execution = runner.Runner(self.selected())
            execution.log = io.StringIO()
            with mock.patch.object(runner.subprocess, "Popen", return_value=Process(100, status=9)), mock.patch.object(runner, "stop_group", side_effect=runner.RunnerError("cleanup failed")), mock.patch.object(execution, "prepare", side_effect=lambda: execution.command(["fixture"])):
                self.assertEqual(execution.run(), 1)
                self.assertTrue(execution.cleanup_failed)
            self.assertIn("status 9", errors.getvalue())
            self.assertIn("cleanup failed", errors.getvalue())

    def test_cancellation_and_previous_signal_handlers(self):
        previous = {number: signal.getsignal(number) for number in (signal.SIGINT, signal.SIGTERM)}
        for number in previous:
            with self.subTest(number=number), self.boundaries():
                execution = runner.Runner(self.selected())
                execution.emulator = Process(102)
                with mock.patch.object(execution, "prepare", side_effect=lambda: execution.cancel(number, None)):
                    self.assertEqual(execution.run(), 128 + number)
        self.assertEqual({number: signal.getsignal(number) for number in previous}, previous)

    def test_cancellation_preserves_recorded_nonzero_test_status(self):
        with self.boundaries():
            execution = runner.Runner(self.selected())
            execution.test_status = 19
            with mock.patch.object(execution, "prepare", side_effect=lambda: execution.cancel(signal.SIGTERM, None)):
                self.assertEqual(execution.run(), 19)

    def test_bounded_command_timeout_stops_owned_command(self):
        with self.boundaries():
            execution = runner.Runner(self.selected())
            process = Process(100)
            process.communicate = mock.Mock(side_effect=subprocess.TimeoutExpired("fixture", 1))
            execution.log = io.StringIO()
            with mock.patch.object(runner.subprocess, "Popen", return_value=process), mock.patch.object(runner.time, "monotonic", side_effect=[0, 0, 180]), mock.patch.object(execution, "prepare", side_effect=lambda: execution.command(["fixture"])):
                self.assertEqual(execution.run(), 1)
            self.assertIn(100, self.stopped)
            self.assertEqual(process.communicate.call_args, mock.call(input=None, timeout=1))

    def test_emulator_exit_during_bounded_command_is_detected(self):
        with self.boundaries():
            execution = runner.Runner(self.selected())
            execution.emulator = Process(101, polls=[7])
            execution.log = io.StringIO()
            process = Process(100)
            process.communicate = mock.Mock(side_effect=subprocess.TimeoutExpired("fixture", 1))
            with mock.patch.object(runner.subprocess, "Popen", return_value=process), mock.patch.object(execution, "prepare", side_effect=lambda: execution.command(["fixture"])):
                self.assertEqual(execution.run(), 1)
            self.assertEqual(self.stopped[-2:], [100, 101])

    def test_command_failure_is_not_ignored(self):
        with self.boundaries():
            execution = runner.Runner(self.selected())
            execution.log = io.StringIO()
            with mock.patch.object(runner.subprocess, "Popen", return_value=Process(100, "failure\n", status=9)), mock.patch.object(execution, "prepare", side_effect=lambda: execution.command(["fixture"])):
                self.assertEqual(execution.run(), 1)
            self.assertEqual(self.stopped[-1], 100)

    def test_port_checks_cover_both_emulator_ports(self):
        connection = mock.MagicMock()
        with mock.patch.object(runner.socket, "socket", return_value=connection):
            runner.check_ports()
        self.assertEqual(connection.__enter__.return_value.bind.call_args_list, [mock.call(("127.0.0.1", 5554)), mock.call(("127.0.0.1", 5555))])
        connection.__enter__.return_value.bind.side_effect = OSError("occupied")
        with mock.patch.object(runner.socket, "socket", return_value=connection), self.assertRaises(runner.RunnerError):
            runner.check_ports()

    def test_active_sdk_executable_is_rejected_without_signaling(self):
        process = Path("/proc/1234")
        with mock.patch.object(Path, "iterdir", return_value=iter([process])), mock.patch.object(Path, "read_text", return_value=USER_PROCESS_STAT), mock.patch.object(Path, "readlink", return_value=self.sdk / "emulator/qemu/linux-x86_64/qemu-system-x86_64"), mock.patch.object(runner.os, "killpg") as terminate:
            with self.assertRaises(runner.RunnerError):
                runner.check_active_emulator(self.sdk / "emulator", mock.Mock())
            terminate.assert_not_called()

    def test_permission_error_uses_only_bounded_privileged_readlink(self):
        process = Path("/proc/1234")
        command = mock.Mock(return_value="/usr/bin/unrelated\n")
        with mock.patch.object(Path, "iterdir", return_value=iter([process])), mock.patch.object(Path, "read_text", return_value=USER_PROCESS_STAT), mock.patch.object(Path, "readlink", side_effect=PermissionError()):
            runner.check_active_emulator(self.sdk / "emulator", command)
        command.assert_called_once_with(["sudo", "--non-interactive", "readlink", "--", "/proc/1234/exe"], timeout=30)

    def test_privileged_active_sdk_executable_is_rejected(self):
        process = Path("/proc/1234")
        command = mock.Mock(return_value=str(self.sdk / "emulator/emulator") + "\n")
        with mock.patch.object(Path, "iterdir", return_value=iter([process])), mock.patch.object(Path, "read_text", return_value=USER_PROCESS_STAT), mock.patch.object(Path, "readlink", side_effect=PermissionError()), self.assertRaises(runner.RunnerError):
            runner.check_active_emulator(self.sdk / "emulator", command)

    def test_remaining_privileged_inspection_failure_is_not_ignored(self):
        for error in (runner.RunnerError("denied"), subprocess.TimeoutExpired("sudo", 30), OSError("unavailable")):
            with self.subTest(error=error), mock.patch.object(Path, "iterdir", return_value=iter([Path("/proc/1234")])), mock.patch.object(Path, "read_text", return_value=USER_PROCESS_STAT), mock.patch.object(Path, "readlink", side_effect=PermissionError()), mock.patch.object(Path, "exists", return_value=True), self.assertRaises(type(error)):
                runner.check_active_emulator(self.sdk / "emulator", mock.Mock(side_effect=error))

    def test_vanished_privileged_inspection_is_an_ordinary_race(self):
        with mock.patch.object(Path, "iterdir", return_value=iter([Path("/proc/1234")])), mock.patch.object(Path, "read_text", return_value=USER_PROCESS_STAT), mock.patch.object(Path, "readlink", side_effect=PermissionError()), mock.patch.object(Path, "exists", return_value=False):
            runner.check_active_emulator(self.sdk / "emulator", mock.Mock(side_effect=runner.RunnerError("vanished")))

    def test_privileged_inspection_requires_one_absolute_executable_path(self):
        for output in ("", "relative/path\n", "/usr/bin/one\n/usr/bin/two\n"):
            with self.subTest(output=output), mock.patch.object(Path, "iterdir", return_value=iter([Path("/proc/1234")])), mock.patch.object(Path, "read_text", return_value=USER_PROCESS_STAT), mock.patch.object(Path, "readlink", side_effect=PermissionError()), self.assertRaises(runner.RunnerError):
                runner.check_active_emulator(self.sdk / "emulator", mock.Mock(return_value=output))

    def test_proved_kernel_thread_has_no_executable_inspection(self):
        for flags in (0x00200000, 0x00200000 | 0x00400000):
            data = f"1234 (kernel command with ) parentheses) S 1 1234 1234 0 -1 {flags}\n"
            command = mock.Mock()
            with self.subTest(flags=flags), mock.patch.object(Path, "iterdir", return_value=iter([Path("/proc/1234")])), mock.patch.object(Path, "read_text", return_value=data), mock.patch.object(Path, "readlink", side_effect=PermissionError()) as readlink:
                runner.check_active_emulator(self.sdk / "emulator", command)
            readlink.assert_not_called()
            command.assert_not_called()

    def test_proved_zombie_has_no_executable_inspection(self):
        data = USER_PROCESS_STAT.replace(") S ", ") Z ")
        command = mock.Mock()
        with mock.patch.object(Path, "iterdir", return_value=iter([Path("/proc/1234")])), mock.patch.object(Path, "read_text", return_value=data), mock.patch.object(Path, "readlink", side_effect=PermissionError()) as readlink, mock.patch.object(Path, "exists", return_value=True):
            runner.check_active_emulator(self.sdk / "emulator", command)
        readlink.assert_not_called()
        command.assert_not_called()

    def test_release_recovery_loads_dispatch_runtime_and_preserves_candidate(self):
        repo = self.root / "candidate"
        repo.mkdir()
        hooks = self.root / "empty-hooks"
        hooks.mkdir()
        environment = {name: value for name, value in os.environ.items() if not name.startswith("GIT_")}
        environment.update({"GIT_CONFIG_NOSYSTEM": "1", "GIT_CONFIG_GLOBAL": os.devnull, "GIT_TERMINAL_PROMPT": "0"})
        def git(*arguments):
            return subprocess.run(["git", *arguments], cwd=repo, env=environment, check=True, capture_output=True, text=True, timeout=10).stdout.strip()
        git("init", "--quiet", "--initial-branch=fixture")
        git("config", "user.name", "Release fixture")
        git("config", "user.email", "release@example.invalid")
        git("config", "core.hooksPath", str(hooks))
        git("config", "commit.gpgSign", "false")
        (repo / "README").write_text("Earlier sealed candidate\n")
        git("add", ".")
        git("commit", "--quiet", "-m", "Create earlier candidate")
        candidate = git("rev-parse", "HEAD")
        runtime_files = {
            "run_android_emulator.py": Path(runner.__file__).read_bytes(),
            "android-emulator-package.xml": runner.METADATA.read_bytes(),
        }
        source = repo / "scripts/ci"
        source.mkdir(parents=True)
        for name, data in runtime_files.items():
            (source / name).write_bytes(data)
        git("add", ".")
        git("commit", "--quiet", "-m", "Add dispatch emulator runtime")
        dispatch = git("rev-parse", "HEAD")
        git("checkout", "--quiet", "--detach", candidate)
        self.assertTrue(all(not (source / name).exists() for name in runtime_files))

        root = Path(__file__).resolve().parents[2]
        workflow = (root / ".github/workflows/release.yml").read_text()
        job = re.search(r"^  package-android:\n(.*?)(?=^  [A-Za-z0-9_-]+:|\Z)", workflow, re.MULTILINE | re.DOTALL).group(1)
        checkout = job.split("      - name: Load immutable dispatch emulator runner\n", 1)[0]
        self.assertIn("ref: ${{ needs.candidate.outputs.source_commit }}", checkout)
        self.assertIn("fetch-depth: 0", checkout)
        def step_command(name):
            body = job.split(f"      - name: {name}\n", 1)[1].split("\n      - ", 1)[0]
            return textwrap.dedent(body.split("        run: |\n", 1)[1])
        environment.update({"GITHUB_SHA": dispatch, "RUNNER_TEMP": str(self.root / "retained")})
        loaded = subprocess.run(["bash", "-c", step_command("Load immutable dispatch emulator runner")], cwd=repo, env=environment, check=True, capture_output=True, text=True, timeout=10)
        preserved = self.root / "retained/synchro-dispatch-emulator"
        for name, data in runtime_files.items():
            self.assertEqual((preserved / name).read_bytes(), data)
            self.assertIn(runner.digest(preserved / name), loaded.stdout)
        self.assertIn(dispatch, loaded.stdout)
        self.assertEqual(git("rev-parse", "HEAD"), candidate)
        self.assertEqual(git("status", "--porcelain"), "")
        self.assertTrue(all(not (source / name).exists() for name in runtime_files))

        host_bin = self.root / "host-bin"
        host_bin.mkdir()
        record = self.root / "command.json"
        python = host_bin / "python3"
        python.write_text(
            "#!" + sys.executable + "\n"
            "import json, os, subprocess, sys\n"
            "record = {'arguments': sys.argv[1:], 'cwd': os.getcwd(), 'head': subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip()}\n"
            "with open(os.environ['COMMAND_RECORD'], 'w') as output: json.dump(record, output)\n"
        )
        python.chmod(0o700)
        environment.update({"PATH": str(host_bin) + os.pathsep + environment.get("PATH", ""), "COMMAND_RECORD": str(record), "ANDROID_SDK_ROOT": str(self.sdk)})
        substitutions = {
            "${{ needs.candidate.outputs.version }}": "1.2.3",
            "${{ needs.candidate.outputs.release_dir_name }}": "synchro-1.2.3",
        }
        for cell, api, image in (("SUP-ANDROID-MIN-001", "24", "24"), ("SUP-ANDROID-CURRENT-001", "37", "37.0"), ("SUP-RN-ANDROID-CURRENT-001", "37", "37.0")):
            with self.subTest(cell=cell):
                command = step_command("Run Android package cell")
                values = {**substitutions, "${{ matrix.cell }}": cell, "${{ matrix.support_api }}": api, "${{ matrix.api }}": image}
                for expression, value in values.items():
                    command = command.replace(expression, value)
                subprocess.run(["bash", "-c", command], cwd=repo, env=environment, check=True, capture_output=True, text=True, timeout=10)
                actual = json.loads(record.read_text())
                self.assertEqual(actual["cwd"], str(repo))
                self.assertEqual(actual["head"], candidate)
                self.assertEqual(actual["arguments"][0], str(preserved / "run_android_emulator.py"))
                selected = runner.arguments(actual["arguments"][1:])
                self.assertEqual((selected.api, selected.image_api), (api, image))
                self.assertEqual(selected.avd_name, f"Synchro_Release_API_{image}")
                self.assertEqual(selected.profile, "pixel_2")
                self.assertIsNone(selected.memory)
                self.assertEqual(selected.command, [
                    "make", "release-run-support-cell", "VERSION=1.2.3", "RELEASE_DIR=dist/releases/synchro-1.2.3",
                    "RELEASE_EVIDENCE_DIR=dist/package-evidence", "RELEASE_CONSUMER_DIR=dist/release-consumer",
                    f"SUPPORT_CELL_ID={cell}", f"SUPPORT_PLATFORM_VERSION={api}",
                ])
        self.assertEqual(git("rev-parse", "HEAD"), candidate)
        self.assertEqual(git("status", "--porcelain"), "")
        candidate_workflow = (root / ".github/workflows/ci.yml").read_text()
        self.assertEqual(candidate_workflow.count("python3 scripts/ci/run_android_emulator.py"), 2)
        scope = {"__file__": str(preserved / "run_android_emulator.py"), "__name__": "recovery_fixture"}
        exec(compile(runtime_files["run_android_emulator.py"], scope["__file__"], "exec"), scope)
        self.assertEqual(scope["METADATA"], preserved / "android-emulator-package.xml")
        (preserved / "android-emulator-package.xml").write_bytes(runtime_files["android-emulator-package.xml"] + b"\n")
        with self.boundaries():
            self.assertEqual(scope["Runner"](self.selected()).run(), 1)
        self.assertEqual(self.calls, [])

    def test_malformed_process_stats_fail_before_link_inspection(self):
        cases = (
            "", "1234 malformed", "1234 (fixture) S 1 1234",
            "1234 (fixture) S 1 1234 1234 0 -1 invalid\n",
            "1234 (fixture) S 1 1234 1234 0 -1 -1\n",
            "4321 (fixture) S 1 1234 1234 0 -1 2097152\n",
            "1234 (fixture) S invalid 1234 1234 0 -1 2097152\n",
        )
        for data in cases:
            command = mock.Mock()
            with self.subTest(data=data), mock.patch.object(Path, "iterdir", return_value=iter([Path("/proc/1234")])), mock.patch.object(Path, "read_text", return_value=data), mock.patch.object(Path, "readlink") as readlink, self.assertRaises(runner.RunnerError):
                runner.check_active_emulator(self.sdk / "emulator", command)
            readlink.assert_not_called()
            command.assert_not_called()

    def test_process_exit_during_stat_read_is_an_ordinary_race(self):
        command = mock.Mock()
        with mock.patch.object(Path, "iterdir", return_value=iter([Path("/proc/1234")])), mock.patch.object(Path, "read_text", side_effect=FileNotFoundError()), mock.patch.object(Path, "readlink") as readlink:
            runner.check_active_emulator(self.sdk / "emulator", command)
        readlink.assert_not_called()
        command.assert_not_called()

    def test_prepare_denied_user_process_uses_initialized_retained_log(self):
        inspect = runner.check_active_emulator
        original_iterdir = Path.iterdir
        original_read_text = Path.read_text
        original_readlink = Path.readlink
        def processes(path):
            return iter([Path("/proc/1234")]) if path == Path("/proc") else original_iterdir(path)
        def read_stat(path, *args, **kwargs):
            return USER_PROCESS_STAT if path == Path("/proc/1234/stat") else original_read_text(path, *args, **kwargs)
        def read_link(path):
            if path == Path("/proc/1234/exe"):
                raise PermissionError()
            return original_readlink(path)
        with self.boundaries(), mock.patch.object(runner, "check_active_emulator", inspect), mock.patch.object(Path, "iterdir", processes), mock.patch.object(Path, "read_text", read_stat), mock.patch.object(Path, "readlink", read_link):
            self.assertEqual(runner.Runner(self.selected()).run(), 0)
        self.assertEqual([command for command, _ in self.commands("sudo")], [["sudo", "--non-interactive", "readlink", "--", "/proc/1234/exe"]] * 2)
        self.assertIn("/usr/bin/fixture", (self.root / "logs/runner.log").read_text())

    def test_cleanup_uses_owned_groups_with_term_and_kill_bounds(self):
        process = Process(321)
        process.wait = mock.Mock(side_effect=[subprocess.TimeoutExpired("fixture", 5), 0])
        with mock.patch.object(runner, "group_alive", side_effect=[True, True, True, False, False]), mock.patch.object(runner.os, "killpg") as terminate, mock.patch.object(runner.time, "monotonic", side_effect=[0, 5, 5, 5, 10]):
            runner.stop_group(process)
        self.assertEqual(terminate.call_args_list, [mock.call(321, signal.SIGTERM), mock.call(321, signal.SIGKILL)])
        self.assertEqual(process.wait.call_args_list, [mock.call(timeout=0), mock.call(timeout=0)])


if __name__ == "__main__":
    unittest.main()
