from __future__ import annotations

import contextlib
import copy
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

from verification import probe_support_environment as probe


REPO_ROOT = Path(__file__).resolve().parents[1]
UDID = "9A4C728F-B0AF-4F82-A6EB-8E922368C976"
OTHER_UDID = "637F98DB-C100-4EFA-A2D6-5BF8B424D190"
RUNTIME = "com.apple.CoreSimulator.SimRuntime.iOS-99-9"
COMMANDS = [
    ["xcodebuild", "-version"],
    ["xcrun", "simctl", "list", "devices", "booted", "-j"],
    ["xcrun", "simctl", "list", "runtimes", "-j"],
]


def command_fixtures(ios: str = "27.0", xcode: str = "27.0") -> list[str]:
    # Actual-shaped tool fixtures have independent origins from expected records.
    return [
        f"Xcode {xcode}\nBuild version 27A266a\n",
        json.dumps({"devices": {
            "com.apple.CoreSimulator.SimRuntime.iOS-16-4": [
                {"udid": OTHER_UDID, "state": "Booted", "isAvailable": True},
            ],
            RUNTIME: [{"udid": UDID, "state": "Booted", "isAvailable": True}],
        }}),
        json.dumps({"runtimes": [
            {"identifier": RUNTIME, "version": ios, "isAvailable": True, "platform": "iOS", "buildversion": "24A335"},
            {"identifier": "com.apple.CoreSimulator.SimRuntime.iOS-16-4", "version": "16.4", "isAvailable": True, "platform": "iOS"},
        ]}),
    ]


def responses(fixtures: list[str]) -> list[subprocess.CompletedProcess]:
    return [subprocess.CompletedProcess(command, 0, stdout=text, stderr="ignored tool diagnostics")
            for command, text in zip(COMMANDS, fixtures)]


class AppleEnvironmentProbeTests(unittest.TestCase):
    def run_probe(self, root: Path, fixtures: list[str], cell: str = "SUP-IOS-CURRENT-001", app: Path | None = None):
        output = root / "measured.json"
        diagnostics = io.StringIO()
        with mock.patch.object(probe.subprocess, "run", side_effect=responses(fixtures)) as command, contextlib.redirect_stderr(diagnostics):
            record = probe.probe_ios(cell, UDID.lower(), output, app)
        self.assertEqual(command.call_args_list, [
            mock.call(arguments, capture_output=True, text=True, check=True, timeout=30) for arguments in COMMANDS
        ])
        self.assertEqual(json.loads(output.read_text()), record)
        self.assertEqual(output.read_text(), json.dumps(record, indent=2, sort_keys=True) + "\n")
        self.assertEqual(set(record), {"id", "environment"})
        self.assertEqual(diagnostics.getvalue(), f"Xcode {record['environment']['xcode']}\nBuild version 27A266a\n")
        self.assertNotIn("buildversion", record["environment"])
        return record

    def assert_rejected(self, root: Path, fixtures: list[str], cell: str = "SUP-IOS-CURRENT-001", app: Path | None = None):
        output = root / "measured.json"
        with mock.patch.object(probe.subprocess, "run", side_effect=responses(fixtures)), self.assertRaises(probe.ProbeError) as raised:
            probe.probe_ios(cell, UDID, output, app)
        self.assertFalse(output.exists())
        self.assertFalse(list(root.glob(".measured.json.*")))
        return str(raised.exception)

    def installed_app(self, root: Path, text: str) -> Path:
        app = root / "app"
        package = app / "node_modules/react-native/package.json"
        package.parent.mkdir(parents=True)
        package.write_text(text)
        return app

    def test_native_minimum_and_current_measure_actual_runtime_and_xcode(self) -> None:
        for cell, ios, xcode in (
            ("SUP-IOS-MIN-001", "16.4", "16.4.1"),
            ("SUP-IOS-CURRENT-001", "27.0", "27.0"),
            ("SUP-IOS-CURRENT-001", "27.0.1", "27.0.1"),
        ):
            with self.subTest(cell=cell, ios=ios), tempfile.TemporaryDirectory() as directory:
                with mock.patch.dict(os.environ, {"SUPPORT_PLATFORM_VERSION": "99.9"}):
                    record = self.run_probe(Path(directory), command_fixtures(ios, xcode), cell)
                self.assertEqual(record, {"id": cell, "environment": {"ios": ios, "xcode": xcode}})

    def test_react_native_reads_installed_runtime_version(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            app = self.installed_app(root, '{"name":"react-native","version":"0.83.11"}')
            # The unrelated template and Synchro metadata do not supply this version.
            (app / "package.json").write_text('{"version":"9.9.9","dependencies":{"react-native":"0.83.10"}}')
            record = self.run_probe(root, command_fixtures(), probe.RN_IOS_CELL, app)
            self.assertEqual(record["environment"], {"ios": "27.0", "xcode": "27.0", "react_native": "0.83.11"})

    def test_invalid_cell_udid_and_app_arguments_reject_before_commands(self) -> None:
        cases = [(cell, UDID, None) for cell in ("", "SUP-ANDROID-CURRENT-001", "SUP-PG-LINUX-X64-001", "SUP-MACOS-CURRENT-001")]
        cases.extend(("SUP-IOS-CURRENT-001", identity, None) for identity in (
            "", "booted", "not-a-uuid", " " + UDID, UDID + "\n", UDID.replace("-", ""), "{" + UDID + "}", None,
        ))
        cases.extend([(probe.RN_IOS_CELL, UDID, None), ("SUP-IOS-CURRENT-001", UDID, Path("app")), ("SUP-IOS-MIN-001", UDID, Path("app"))])
        for cell, identity, app in cases:
            with self.subTest(cell=cell, identity=identity), tempfile.TemporaryDirectory() as directory:
                output = Path(directory) / "measured.json"
                with mock.patch.object(probe.subprocess, "run") as command, self.assertRaises(probe.ProbeError):
                    probe.probe_ios(cell, identity, output, app)
                command.assert_not_called()
                self.assertFalse(output.exists())

    def test_wrong_duplicate_unavailable_and_unbooted_devices_reject(self) -> None:
        for mutation in ("wrong", "duplicate", "duplicate-group", "unavailable", "unbooted", "missing-udid", "bad-udid", "missing-state", "missing-availability"):
            fixtures = command_fixtures()
            devices = json.loads(fixtures[1])
            selected = devices["devices"][RUNTIME][0]
            if mutation == "wrong":
                selected["udid"] = OTHER_UDID
            elif mutation == "duplicate":
                devices["devices"][RUNTIME].append(copy.deepcopy(selected))
            elif mutation == "duplicate-group":
                devices["devices"]["different-runtime"] = [{**selected, "udid": UDID.lower()}]
            elif mutation == "unavailable":
                selected["isAvailable"] = False
            elif mutation == "unbooted":
                selected["state"] = "Shutdown"
            elif mutation == "bad-udid":
                selected["udid"] = "bad"
            else:
                selected.pop({"missing-udid": "udid", "missing-state": "state", "missing-availability": "isAvailable"}[mutation])
            fixtures[1] = json.dumps(devices)
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as directory:
                self.assert_rejected(Path(directory), fixtures)

    def test_wrong_duplicate_unavailable_and_missing_runtimes_reject(self) -> None:
        for mutation in ("wrong-id", "duplicate", "unavailable", "wrong-platform", "missing-version", "nonstring-version", "missing-availability", "missing-platform", "no-match"):
            fixtures = command_fixtures()
            runtimes = json.loads(fixtures[2])
            runtime = runtimes["runtimes"][0]
            if mutation == "wrong-id":
                runtime["identifier"] = "different-runtime"
            elif mutation == "duplicate":
                runtimes["runtimes"].append(copy.deepcopy(runtime))
            elif mutation == "unavailable":
                runtime["isAvailable"] = False
            elif mutation == "wrong-platform":
                runtime["platform"] = "tvOS"
            elif mutation == "nonstring-version":
                runtime["version"] = 27
            elif mutation == "no-match":
                runtimes["runtimes"] = []
            else:
                runtime.pop({"missing-version": "version", "missing-availability": "isAvailable", "missing-platform": "platform"}[mutation])
            fixtures[2] = json.dumps(runtimes)
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as directory:
                self.assert_rejected(Path(directory), fixtures)

    def test_missing_malformed_and_duplicate_json_metadata_reject(self) -> None:
        for position, text in (
            (1, "not JSON"), (2, "null"), (1, "[]"), (1, '{}'), (2, '{}'),
            (1, '{"devices":[],"devices":{}}'), (2, '{"runtimes":[],"runtimes":[]}'),
            (1, json.dumps({"devices": {RUNTIME: {}}})),
            (1, json.dumps({"devices": {RUNTIME: [None]}})),
            (2, '{"runtimes":[null]}'),
            (2, command_fixtures()[2].replace('"version": "27.0"', '"version": "27.0", "version": "27.0"')),
            (1, command_fixtures()[1].replace(f'"udid": "{UDID}"', f'"udid": "{UDID}", "udid": "{UDID}"')),
        ):
            fixtures = command_fixtures()
            fixtures[position] = text
            with self.subTest(position=position, text=text), tempfile.TemporaryDirectory() as directory:
                message = self.assert_rejected(Path(directory), fixtures)
                self.assertNotIn(text, message)

    def test_xcode_requires_version_and_associated_nonempty_build(self) -> None:
        for text in ("", "Xcode 27.0\n", "Build version 27A266a\n", "Xcode 27.0\nBuild version \n",
                     "Xcode 27.0\nBuild version   \n", "Xcode 27.0\nBuild version a\nBuild version b\n",
                     "Xcode 27.0\nBuild version " + "a" * 201, "Xcode 27.0\nBuild version a\x00b"):
            fixtures = command_fixtures()
            fixtures[0] = text
            with self.subTest(text=text), tempfile.TemporaryDirectory() as directory:
                self.assert_rejected(Path(directory), fixtures)

    def test_shared_validator_rejects_unsupported_or_noncanonical_apple_versions(self) -> None:
        for cell, ios, xcode in (
            ("SUP-IOS-MIN-001", "17.0", "27.0"), ("SUP-IOS-MIN-001", "15.9", "27.0"),
            ("SUP-IOS-CURRENT-001", "15.9", "27.0"), ("SUP-IOS-CURRENT-001", "027.0", "27.0"),
            ("SUP-IOS-CURRENT-001", "27.0-beta", "27.0"), ("SUP-IOS-CURRENT-001", "27.0", "27"),
            ("SUP-IOS-CURRENT-001", "27.0", "27.0-beta"), ("SUP-IOS-CURRENT-001", "27.0", "current"),
        ):
            with self.subTest(cell=cell, ios=ios, xcode=xcode), tempfile.TemporaryDirectory() as directory:
                self.assert_rejected(Path(directory), command_fixtures(ios, xcode), cell)

    def test_installed_react_native_missing_malformed_and_duplicate_metadata_reject(self) -> None:
        for text in (None, "{", "[]", '{}', '{"version":null}', '{"version":"0.84.0"}',
                     '{"version":"0.083.10"}', '{"version":"0.83"}', '{"version":"current"}',
                     '{"version":"0.83.10","version":"0.83.10"}', '{"metadata":{"a":1,"a":1},"version":"0.83.10"}'):
            with self.subTest(text=text), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                app = root / "missing-app" if text is None else self.installed_app(root, text)
                self.assert_rejected(root, command_fixtures(), probe.RN_IOS_CELL, app)

    def test_installed_react_native_xcode_supported_patch_boundary(self) -> None:
        for xcode, version, accepted in (("26.3", "0.83.4", True), ("26.4", "0.83.4", False),
                                         ("26.4.0", "0.83.4", False), ("26.4", "0.83.5", True), ("27.0", "0.83.5", True)):
            with self.subTest(xcode=xcode, version=version), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                app = self.installed_app(root, json.dumps({"version": version}))
                if accepted:
                    record = self.run_probe(root, command_fixtures(xcode=xcode), probe.RN_IOS_CELL, app)
                    self.assertEqual(record["environment"]["react_native"], version)
                else:
                    self.assert_rejected(root, command_fixtures(xcode=xcode), probe.RN_IOS_CELL, app)

    def test_each_command_failure_timeout_and_unreadable_text_are_bounded(self) -> None:
        secret = "arbitrary private tool output"
        for index, arguments in enumerate(COMMANDS):
            for error in (subprocess.CalledProcessError(7, arguments, output=secret, stderr=secret),
                          subprocess.TimeoutExpired(arguments, 30, output=secret, stderr=secret),
                          OSError(secret), UnicodeError(secret)):
                with self.subTest(command=arguments, error=type(error)), tempfile.TemporaryDirectory() as directory:
                    output = Path(directory) / "measured.json"
                    before = responses(command_fixtures())[:index]
                    with mock.patch.object(probe.subprocess, "run", side_effect=[*before, error]) as command, self.assertRaises(probe.ProbeError) as raised:
                        probe.probe_ios("SUP-IOS-CURRENT-001", UDID, output)
                    self.assertEqual(command.call_count, index + 1)
                    self.assertNotIn(secret, str(raised.exception))
                    self.assertLess(len(str(raised.exception)), 100)
                    self.assertFalse(output.exists())

    def test_atomic_replacement_and_failed_replacement_cleanup(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            output = root / "nested/measured.json"
            record = {"id": "SUP-IOS-MIN-001", "environment": {"ios": "16.4", "xcode": "16.4"}}
            with mock.patch.object(probe.os, "replace", wraps=os.replace) as replace:
                probe.write_record(output, record)
            source, destination = replace.call_args.args
            self.assertEqual(source.parent, output.parent)
            self.assertEqual(destination, output)
            self.assertFalse(source.exists())
            original = output.read_bytes()
            with mock.patch.object(probe.os, "replace", side_effect=OSError("private details")), self.assertRaises(probe.ProbeError):
                probe.write_record(output, {"id": "replacement"})
            self.assertEqual(output.read_bytes(), original)
            self.assertEqual(list(output.parent.iterdir()), [output])

    def test_cli_success_and_bounded_failure(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "measured.json"
            arguments = ["probe", "ios", "--cell", "SUP-IOS-CURRENT-001", "--simulator-udid", UDID, "--output", str(output)]
            with mock.patch.object(sys, "argv", arguments), mock.patch.object(probe.subprocess, "run", side_effect=responses(command_fixtures())), contextlib.redirect_stderr(io.StringIO()):
                self.assertEqual(probe.main(), 0)
            output.unlink()
            diagnostics = io.StringIO()
            with mock.patch.object(sys, "argv", arguments), mock.patch.object(probe.subprocess, "run", side_effect=OSError("private details")), contextlib.redirect_stderr(diagnostics):
                self.assertEqual(probe.main(), 1)
            self.assertNotIn("private details", diagnostics.getvalue())
            self.assertFalse(output.exists())

    def test_direct_script_imports_local_validator_from_another_directory(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "support_environments.py").write_text("raise AssertionError('wrong validator')\n")
            environment = {**os.environ, "PYTHONPATH": str(root), "PYTHONDONTWRITEBYTECODE": "1"}
            # Help returns before measurement; no platform tool can be invoked here.
            result = subprocess.run([sys.executable, str(REPO_ROOT / "verification/probe_support_environment.py"), "ios", "--help"],
                                    cwd=root, env=environment, capture_output=True, text=True, timeout=30)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("--simulator-udid", result.stdout)
            self.assertIn("--react-native-app", result.stdout)


class PostgreSQLEnvironmentProbeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.output = self.root / "measured.json"
        self.bindir = Path("/retained/pg18/bin")
        self.connection = {
            "PGDATABASE": "postgres://127.0.0.1:54329/synchro_cf?sslmode=disable",
            "PGUSER": "fixture_admin", "PGPASSWORD": "fixture_private_password",
            "PATH": "/fixture/bin", "HOME": "/fixture/home", "UNRELATED": "preserved",
            "PGHOST": "remote.invalid", "PGHOSTADDR": "192.0.2.1", "PGPORT": "9999",
            "PGSERVICE": "unrelated", "PGSERVICEFILE": "/private/service", "PGPASSFILE": "/private/password",
            "PGOPTIONS": "-c statement_timeout=0", "PGCONNECT_TIMEOUT": "0", "PGSSLMODE": "require",
        }
        self.enterContext(mock.patch.object(probe.os, "environ", dict(self.connection)))
        self.system = self.enterContext(mock.patch.object(probe.platform, "system", return_value="Linux"))
        self.machine = self.enterContext(mock.patch.object(probe.platform, "machine", return_value="x86_64"))
        self.release = self.enterContext(mock.patch.object(probe.platform, "freedesktop_os_release", return_value={
            "ID": "ubuntu", "VERSION_ID": "24.04", "PRETTY_NAME": "Ignored display name", "VERSION": "Ignored kernel selector",
        }))
        self.files = self.enterContext(mock.patch.object(Path, "is_file", return_value=True))
        self.access = self.enterContext(mock.patch.object(probe.os, "access", return_value=True))
        self.command = self.enterContext(mock.patch.object(probe.subprocess, "run"))
        self.diagnostics = io.StringIO()
        self.enterContext(contextlib.redirect_stderr(self.diagnostics))
        self.commands = [
            [str(self.bindir / "postgres"), "--version"],
            [str(self.bindir / "psql"), "-X", "-A", "-t", "--no-password", "-v", "ON_ERROR_STOP=1", "-c", probe.PG_VERSION_QUERY],
        ]
        self.fixtures()

    def fixtures(self, binary: str = "postgres (PostgreSQL) 18.3 (Ubuntu 18.3-1.pgdg24.04+1)\n",
                 server: str = '{"version":"18.3 (Debian 18.3-1)","version_num":"180003"}\n') -> None:
        # Installed and actual-running outputs are independently authored fixtures.
        self.command.reset_mock()
        self.command.side_effect = [subprocess.CompletedProcess(command, 0, stdout=text, stderr="private arbitrary output")
                                    for command, text in zip(self.commands, (binary, server))]

    def measure(self):
        return probe.probe_postgresql(probe.PG_CELL, self.bindir, self.output)

    def rejected(self) -> str:
        with self.assertRaises(probe.ProbeError) as raised:
            self.measure()
        self.assertFalse(self.output.exists())
        self.assertFalse(list(self.root.glob(".measured.json.*")))
        for private in (self.connection["PGUSER"], self.connection["PGPASSWORD"], self.connection["PGDATABASE"], "private arbitrary output"):
            self.assertNotIn(private, str(raised.exception))
        return str(raised.exception)

    def test_success_retains_actual_linux_and_matching_running_version(self) -> None:
        before = dict(probe.os.environ)
        record = self.measure()
        self.assertEqual(record, {"id": "SUP-PG-LINUX-X64-001", "environment": {
            "architecture": "x86_64", "os": "ubuntu-24.04", "postgresql": "18.3",
        }})
        self.assertEqual(self.output.read_text(), json.dumps(record, indent=2, sort_keys=True) + "\n")
        self.assertEqual(self.diagnostics.getvalue(), "PostgreSQL 18.3; Linux x86_64; ubuntu-24.04\n")
        self.assertEqual(dict(probe.os.environ), before)
        clean = {name: value for name, value in self.connection.items() if not name.startswith("PG")}
        query = {**clean, "PGHOST": "127.0.0.1", "PGPORT": "54329", "PGDATABASE": "synchro_cf", "PGSSLMODE": "disable",
                 "PGUSER": "fixture_admin", "PGPASSWORD": "fixture_private_password",
                 "PGCONNECT_TIMEOUT": "10", "PGOPTIONS": "-c default_transaction_read_only=on -c statement_timeout=10000"}
        self.assertEqual(self.command.call_args_list, [
            mock.call(self.commands[0], capture_output=True, text=True, check=True, timeout=30, env=clean),
            mock.call(self.commands[1], capture_output=True, text=True, check=True, timeout=30, env=query),
        ])
        self.assertEqual(self.access.call_args_list, [mock.call(self.bindir / name, os.X_OK) for name in ("postgres", "psql")])
        for call in self.command.call_args_list:
            self.assertNotIn(self.connection["PGDATABASE"], call.args[0])
            self.assertNotIn(self.connection["PGPASSWORD"], call.args[0])
        self.assertEqual({name for name in query if name.startswith("PG")}, {
            "PGHOST", "PGPORT", "PGDATABASE", "PGSSLMODE", "PGUSER", "PGPASSWORD", "PGCONNECT_TIMEOUT", "PGOPTIONS",
        })

    def test_percent_encoded_database_path_uses_decoded_query_environment(self) -> None:
        uri = "postgresql://127.0.0.1:55432/synchro%20fixture%2F%C3%A9%2B?sslmode=disable"
        with mock.patch.dict(probe.os.environ, {"PGDATABASE": uri}):
            before = dict(probe.os.environ)
            self.measure()
            clean = {"PATH": "/fixture/bin", "HOME": "/fixture/home", "UNRELATED": "preserved"}
            query = {**clean, "PGHOST": "127.0.0.1", "PGPORT": "55432", "PGDATABASE": "synchro fixture/é+",
                     "PGSSLMODE": "disable", "PGUSER": "fixture_admin", "PGPASSWORD": "fixture_private_password",
                     "PGCONNECT_TIMEOUT": "10", "PGOPTIONS": "-c default_transaction_read_only=on -c statement_timeout=10000"}
            self.assertEqual(self.command.call_args_list, [
                mock.call(self.commands[0], capture_output=True, text=True, check=True, timeout=30, env=clean),
                mock.call(self.commands[1], capture_output=True, text=True, check=True, timeout=30, env=query),
            ])
            self.assertNotEqual(query["PGDATABASE"], uri)
            self.assertEqual(dict(probe.os.environ), before)

    def test_canonical_patch_zero_and_optional_bounded_distribution_suffix(self) -> None:
        for uri, binary, server in (
            ("postgresql://127.0.0.1:54329/synchro_cf?sslmode=disable", "postgres (PostgreSQL) 18.0\n", '{"version":"18.0","version_num":"180000"}'),
            (self.connection["PGDATABASE"], "postgres (PostgreSQL) 18.10\n", '{"version":"18.10 (Ubuntu)","version_num":"180010"}'),
        ):
            with self.subTest(binary=binary), mock.patch.dict(probe.os.environ, {"PGDATABASE": uri}):
                self.fixtures(binary, server)
                record = self.measure()
                self.assertEqual(record["environment"]["postgresql"], json.loads(server)["version"].split(" ")[0])
                self.output.unlink()

    def test_missing_credentials_reject_before_any_command(self) -> None:
        for name in ("PGDATABASE", "PGUSER", "PGPASSWORD"):
            for value in (None, "", "bad\x00value"):
                with self.subTest(name=name, value=value), mock.patch.dict(probe.os.environ, {}, clear=False):
                    if value is None:
                        del probe.os.environ[name]
                    else:
                        probe.os.environ[name] = value
                    self.fixtures()
                    self.rejected()
                    self.command.assert_not_called()

    def test_unsafe_malformed_or_nonprovisioner_uris_reject_before_commands(self) -> None:
        for uri in (
            "host=127.0.0.1 dbname=fixture", "https://127.0.0.1:5432/db?sslmode=disable",
            "postgres://user@127.0.0.1:5432/db?sslmode=disable", "postgres://user:password@127.0.0.1:5432/db?sslmode=disable",
            "postgres://localhost:5432/db?sslmode=disable", "postgres://192.0.2.1:5432/db?sslmode=disable",
            "postgres://[::1]:5432/db?sslmode=disable", "postgres://127.0.0.1/db?sslmode=disable",
            "postgres://127.0.0.1:0/db?sslmode=disable", "postgres://127.0.0.1:65536/db?sslmode=disable",
            "postgres://127.0.0.1:bad/db?sslmode=disable", "postgres://127.0.0.1:5432/?sslmode=disable",
            "postgres://127.0.0.1:5432/db", "postgres://127.0.0.1:5432/db?sslmode=require",
            "postgres://127.0.0.1:5432/db?sslmode=disable&sslmode=disable",
            "postgres://127.0.0.1:5432/db?sslmode=disable&host=remote.invalid",
            "postgres://127.0.0.1:5432/db?sslmode=disable&", "postgres://127.0.0.1:5432/db?sslmode=disable#",
            "postgres://127.0.0.1:5432/db?sslmode=disable#fragment", " postgres://127.0.0.1:5432/db?sslmode=disable",
            "postgres://127.0.0.1:5432/db?sslmode=disable\n", "postgres://127.0.0.1:5432/bad%zz?sslmode=disable",
            "postgres://127.0.0.1:5432/bad%00?sslmode=disable", "postgres://[bad/db?sslmode=disable",
        ):
            with self.subTest(uri=uri), mock.patch.dict(probe.os.environ, {"PGDATABASE": uri}):
                self.fixtures()
                message = self.rejected()
                self.assertNotIn(uri, message)
                self.command.assert_not_called()

    def test_wrong_linux_profile_and_unreadable_os_release_reject_before_commands(self) -> None:
        for target, value in ((self.system, "Darwin"), (self.machine, "arm64"), (self.machine, "amd64"),
                              (self.release, {}), (self.release, None), (self.release, {"ID": "debian", "VERSION_ID": "24.04"}),
                              (self.release, {"ID": "ubuntu", "VERSION_ID": "22.04"}),
                              (self.release, {"ID": "ubuntu", "PRETTY_NAME": "Ubuntu 24.04"})):
            original = target.return_value
            with self.subTest(value=value):
                target.return_value = value
                self.fixtures()
                self.rejected()
                self.command.assert_not_called()
                target.return_value = original
        for error in (OSError("private arbitrary output"), ValueError("private arbitrary output"), UnicodeError("private arbitrary output")):
            with self.subTest(error=type(error)):
                self.release.side_effect = error
                self.rejected()
                self.command.assert_not_called()
        self.release.side_effect = None

    def test_retained_postgres_and_psql_must_be_executable_files(self) -> None:
        for file_results, access_results in (([False], []), ([True, False], [True]), ([True], [False]), ([True, True], [True, False])):
            with self.subTest(files=file_results, access=access_results):
                self.files.side_effect = file_results
                self.access.side_effect = access_results
                self.rejected()
                self.command.assert_not_called()

    def test_prerelease_malformed_multiline_and_ambiguous_versions_reject(self) -> None:
        for version in ("17.3", "18", "18.03", "18.3.0", "18.3beta1", "18.3-rc1", "18.3\n", "18.3\n18.3",
                        "18.3 ()", "18.3 ( )", "18.3 ((Ubuntu))", "18.3 (Ubuntu) extra", "18.3 (bad\x00suffix)", "18.3 (Ubuntu\u2028suffix)",
                        "18.3 (" + "a" * 201 + ")"):
            for phase in ("binary", "server"):
                with self.subTest(version=version, phase=phase):
                    binary = f"postgres (PostgreSQL) {version}\n" if phase == "binary" else "postgres (PostgreSQL) 18.3\n"
                    server = json.dumps({"version": version if phase == "server" else "18.3", "version_num": "180003"})
                    self.fixtures(binary, server)
                    self.rejected()
        for binary in ("psql (PostgreSQL) 18.3\n", "18.3\n", "postgres (PostgreSQL) 18.3\npostgres (PostgreSQL) 18.3\n"):
            self.fixtures(binary=binary)
            self.rejected()

    def test_running_server_mismatch_and_version_number_mismatch_reject(self) -> None:
        for server in ('{"version":"18.4","version_num":"180004"}', '{"version":"18.3","version_num":"180004"}',
                       '{"version":"18.3","version_num":"0180003"}', '{"version":"18.3","version_num":"180003.0"}',
                       '{"version":"18.3","version_num":"180003\\n"}'):
            with self.subTest(server=server):
                self.fixtures(server=server)
                self.rejected()

    def test_missing_extra_malformed_nonstring_and_duplicate_server_fields_reject(self) -> None:
        for server in ("not JSON", "[]", "null", "{}", '{"version":"18.3"}', '{"version":"18.3","version_num":180003}',
                       '{"version":"","version_num":"180003"}', '{"version":null,"version_num":"180003"}',
                       '{"version":"18.3","version_num":""}', '{"version":"18.3","version_num":"180003","extra":"1"}',
                       '{"version":"18.3","version":"18.3","version_num":"180003"}',
                       '{"version":"18.3","version_num":"180003","version_num":"180003"}',
                       '{"version":"18.3","version_num":"180003"}\n{"version":"18.3","version_num":"180003"}'):
            with self.subTest(server=server):
                self.fixtures(server=server)
                self.rejected()

    def test_each_command_failure_and_timeout_are_credential_safe(self) -> None:
        secret = "private arbitrary output " + self.connection["PGPASSWORD"]
        for index, command in enumerate(self.commands):
            for error in (subprocess.CalledProcessError(1, command, output=secret, stderr=secret),
                          subprocess.TimeoutExpired(command, 30, output=secret, stderr=secret), OSError(secret), UnicodeError(secret)):
                with self.subTest(index=index, error=type(error)):
                    self.fixtures()
                    before = [subprocess.CompletedProcess(self.commands[0], 0, stdout="postgres (PostgreSQL) 18.3\n")] if index else []
                    self.command.side_effect = [*before, error]
                    self.rejected()
                    self.assertEqual(self.command.call_count, index + 1)
                    self.assertEqual(self.diagnostics.getvalue(), "")

    def test_postgresql_cli_and_wrong_cell_rejection(self) -> None:
        arguments = ["probe", "postgresql", "--cell", probe.PG_CELL, "--pg18-bindir", str(self.bindir), "--output", str(self.output)]
        with mock.patch.object(sys, "argv", arguments):
            self.assertEqual(probe.main(), 0)
        self.output.unlink()
        self.fixtures()
        with self.assertRaises(probe.ProbeError):
            probe.probe_postgresql("SUP-IOS-CURRENT-001", self.bindir, self.output)
        self.command.assert_not_called()
        self.assertFalse(self.output.exists())
        for missing in ("--cell", "--pg18-bindir", "--output"):
            index = arguments.index(missing)
            with self.subTest(missing=missing), mock.patch.object(sys, "argv", arguments[:index] + arguments[index + 2:]), self.assertRaises(SystemExit) as raised:
                probe.main()
            self.assertEqual(raised.exception.code, 2)
            self.command.assert_not_called()


if __name__ == "__main__":
    unittest.main()
