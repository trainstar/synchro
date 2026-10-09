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
            "com.apple.CoreSimulator.SimRuntime.iOS-17-0": [
                {"udid": OTHER_UDID, "state": "Booted", "isAvailable": True},
            ],
            RUNTIME: [{"udid": UDID, "state": "Booted", "isAvailable": True}],
        }}),
        json.dumps({"runtimes": [
            {"identifier": RUNTIME, "version": ios, "isAvailable": True, "platform": "iOS", "buildversion": "24A335"},
            {"identifier": "com.apple.CoreSimulator.SimRuntime.iOS-17-0", "version": "17.0", "isAvailable": True, "platform": "iOS"},
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
            ("SUP-IOS-MIN-001", "17.0", "27.0"),
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

    def test_minimum_react_native_requires_exact_installed_runtime(self) -> None:
        for version in ("0.82.1", "0.82.0", "0.82.2", "0.83.10", None):
            with self.subTest(version=version), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                app = root / "missing-app" if version is None else self.installed_app(root, json.dumps({"version": version}))
                if version == "0.82.1":
                    record = self.run_probe(root, command_fixtures(), "SUP-RN-IOS-MIN-001", app)
                    self.assertEqual(record, {"id": "SUP-RN-IOS-MIN-001", "environment": {
                        "ios": "27.0", "xcode": "27.0", "react_native": "0.82.1",
                    }})
                else:
                    self.assert_rejected(root, command_fixtures(), "SUP-RN-IOS-MIN-001", app)

    def test_invalid_cell_udid_and_app_arguments_reject_before_commands(self) -> None:
        cases = [(cell, UDID, None) for cell in ("", "SUP-ANDROID-CURRENT-001", "SUP-PG-LINUX-X64-001", "SUP-MACOS-CURRENT-001")]
        cases.extend((cell, UDID, Path("app")) for cell in ("SUP-RN-ANDROID-MIN-001", "SUP-RN-IOS-MIN-002"))
        cases.extend(("SUP-IOS-CURRENT-001", identity, None) for identity in (
            "", "booted", "not-a-uuid", " " + UDID, UDID + "\n", UDID.replace("-", ""), "{" + UDID + "}", None,
        ))
        cases.extend([(probe.RN_IOS_CELL, UDID, None), ("SUP-RN-IOS-MIN-001", UDID, None),
                      ("SUP-IOS-CURRENT-001", UDID, Path("app")), ("SUP-IOS-MIN-001", UDID, Path("app"))])
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
            ("SUP-IOS-MIN-001", "18.0", "27.0"), ("SUP-IOS-MIN-001", "16.4", "27.0"),
            ("SUP-IOS-MIN-001", "15.9", "27.0"),
            ("SUP-IOS-CURRENT-001", "15.9", "27.0"), ("SUP-IOS-CURRENT-001", "027.0", "27.0"),
            ("SUP-IOS-CURRENT-001", "27.0-beta", "27.0"), ("SUP-IOS-CURRENT-001", "27.0", "27"),
            ("SUP-IOS-CURRENT-001", "27.0", "27.0-beta"), ("SUP-IOS-CURRENT-001", "27.0", "current"),
        ):
            with self.subTest(cell=cell, ios=ios, xcode=xcode), tempfile.TemporaryDirectory() as directory:
                self.assert_rejected(Path(directory), command_fixtures(ios, xcode), cell)

    def test_installed_react_native_missing_malformed_and_duplicate_metadata_reject(self) -> None:
        for text in (None, "{", "[]", '{}', '{"version":null}', '{"version":"0.84.0"}', '{"version":"0.82.1"}',
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
            record = {"id": "SUP-IOS-MIN-001", "environment": {"ios": "17.0", "xcode": "27.0"}}
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


class AndroidEnvironmentProbeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.sdk = self.root / "sdk"
        self.adb = self.sdk / "platform-tools/adb"
        self.engine = self.sdk / "emulator/qemu/linux-x86_64/qemu-system-x86_64"
        for binary in (self.adb, self.engine):
            binary.parent.mkdir(parents=True, exist_ok=True)
            binary.write_text("fixture executable\n", encoding="utf-8")
            binary.chmod(0o755)
        self.avd = self.root / "Selected.avd"
        self.avd.mkdir()
        self.name = "Selected_API"
        self.config = self.avd / "config.ini"
        self.runtime = self.avd / "hardware-qemu.ini"
        self.emulator_xml = self.sdk / "emulator/package.xml"
        self.emulator_xml.write_text(self.package("emulator", "<major>37</major><minor>2</minor><micro>12</micro>"), encoding="utf-8")
        self.image_fixture("37.0", "6")
        self.proc = self.root / "proc"
        (self.proc / "net").mkdir(parents=True)
        self.sockets()
        self.process(321)
        self.devices = "List of devices attached\nemulator-5554 device product:sdk_gphone model:sdk_gphone device:generic transport_id:1\n\n"
        self.api = "37\n"
        self.console_name = self.name + "\r\nOK\r\n"
        self.console_path = str(self.avd) + "\r\nOK\r\n"
        self.version = "Android emulator version 37.2.12.0 (build_id 16428233) (CL:N/A)\nCopyright vendor\n"
        self.output = self.root / "environment.json"
        self.identity = self.root / "identity.json"
        self.app = self.root / "app"
        package = self.app / "node_modules/react-native/package.json"
        package.parent.mkdir(parents=True)
        package.write_text('{"name":"react-native","version":"0.83.5"}', encoding="utf-8")
        self.diagnostics = io.StringIO()
        for patcher in (
            mock.patch.dict(os.environ, {"KEEP_ME": "retained"}, clear=True),
            mock.patch.object(probe, "PROC_ROOT", self.proc),
            mock.patch.object(probe.platform, "system", return_value="Linux"),
            mock.patch.object(probe.platform, "machine", return_value="x86_64"),
            mock.patch.object(probe.platform, "freedesktop_os_release", return_value={"ID": "ubuntu", "VERSION_ID": "24.04"}),
            contextlib.redirect_stderr(self.diagnostics),
        ):
            patcher.__enter__()
            self.addCleanup(patcher.__exit__, None, None, None)
        self.command = mock.patch.object(probe.subprocess, "run", side_effect=self.execute).start()
        self.addCleanup(mock.patch.stopall)

    @staticmethod
    def package(path: str, revision: str) -> str:
        return f'<sdk:sdk-repository xmlns:sdk="http://schemas.android.com/sdk/android/repo/repository2/03"><localPackage path="{path}"><revision>{revision}</revision></localPackage></sdk:sdk-repository>'

    def image_fixture(self, api: str, revision: str) -> None:
        self.image = self.sdk / f"system-images/android-{api}/google_apis/x86_64"
        self.image.mkdir(parents=True, exist_ok=True)
        (self.image / "system.img").write_text("image fixture; not hashed", encoding="utf-8")
        self.image_xml = self.image / "package.xml"
        self.image_xml.write_text(self.package(f"system-images;android-{api};google_apis;x86_64", f"<major>{revision}</major>"), encoding="utf-8")
        self.config.write_text(f"avd.id=<build>\navd.name=<build>\nimage.sysdir.1={self.image.relative_to(self.sdk)}/\n", encoding="utf-8")
        self.runtime.write_text(f"hw.cpu.arch=x86_64\navd.name={self.name}\ndisk.systemPartition.initPath={self.image}/system.img\n", encoding="utf-8")

    def sockets(self, inodes: tuple[int, ...] = (9001, 9002)) -> None:
        header = "  sl  local_address rem_address st tx_queue rx_queue tr tm->when retrnsmt uid timeout inode\n"
        for name, inode, address in zip(("tcp", "tcp6"), inodes, ("0100007F", "00000000000000000000000001000000")):
            (self.proc / "net" / name).write_text(header + f"  0: {address}:15B2 00000000:0000 0A 00000000:00000000 00:00000000 00000000 {os.getuid()} 0 {inode} 1\n", encoding="utf-8")

    def process(self, pid: int, *, start: str = "123456", selectors: list[str] | None = None) -> Path:
        process = self.proc / str(pid)
        (process / "fd").mkdir(parents=True, exist_ok=True)
        for number, inode in enumerate((9001, 9002)):
            fd = process / "fd" / str(number)
            fd.unlink(missing_ok=True)
            fd.symlink_to(f"socket:[{inode}]")
        exe = process / "exe"
        exe.unlink(missing_ok=True)
        exe.symlink_to(self.engine)
        # Field 22 follows state and the 18 fields from ppid through itrealvalue.
        (process / "stat").write_text(f"{pid} (qemu (worker)) S " + "0 " * 18 + start + " 0 0\n", encoding="utf-8")
        (process / "cmdline").write_bytes(("\x00".join([str(self.engine), *(selectors if selectors is not None else ["-avd", self.name]), "-port", "5554"]) + "\x00").encode())
        return process

    def execute(self, command: list[str], **options: object) -> subprocess.CompletedProcess:
        self.assertEqual(options["timeout"], 30)
        self.assertTrue(options["check"])
        self.assertTrue(options["capture_output"])
        self.assertTrue(options["text"])
        if command == [str(self.adb), "-L", "tcp:127.0.0.1:5037", "devices", "-l"]:
            result = self.devices
        elif command == [str(self.adb), "-L", "tcp:127.0.0.1:5037", "-s", "emulator-5554", "shell", "getprop", "ro.build.version.sdk"]:
            result = self.api
        elif command == [str(self.adb), "-L", "tcp:127.0.0.1:5037", "-s", "emulator-5554", "emu", "avd", "name"]:
            result = self.console_name
        elif command == [str(self.adb), "-L", "tcp:127.0.0.1:5037", "-s", "emulator-5554", "emu", "avd", "path"]:
            result = self.console_path
        elif command == [str(self.proc / "321/exe"), "-version"] or command == [str(self.proc / "322/exe"), "-version"]:
            self.assertEqual(options["env"]["LD_LIBRARY_PATH"], f"{self.sdk}/emulator/lib64:{self.sdk}/emulator/lib64/qt/lib")
            self.assertEqual(options["env"]["KEEP_ME"], "retained")
            result = self.version
        else:
            self.fail(f"unexpected or unscoped fixture command: {command}")
        return subprocess.CompletedProcess(command, 0, result, "")

    def measure(self, cell: str = "SUP-ANDROID-CURRENT-001", **options: object) -> dict:
        return probe.probe_android(cell, self.sdk, "emulator-5554", self.output, self.identity, **options)

    def rejected(self, **options: object) -> None:
        before = {path: path.read_bytes() if path.exists() else None for path in (self.output, self.identity)}
        with self.assertRaises(probe.ProbeError):
            self.measure(**options)
        for path, content in before.items():
            if content is None:
                self.assertFalse(path.exists())
            else:
                self.assertEqual(path.read_bytes(), content)

    def initial(self) -> dict[str, Path]:
        self.measure()
        environment, identity = self.root / "initial.json", self.root / "initial-identity.json"
        environment.write_bytes(self.output.read_bytes())
        identity.write_bytes(self.identity.read_bytes())
        self.output.write_text("previous environment", encoding="utf-8")
        self.identity.write_text("previous identity", encoding="utf-8")
        return {"initial_environment": environment, "initial_identity": identity}

    def test_current_measurement_binds_device_process_and_four_files(self) -> None:
        record = self.measure()
        self.assertEqual(record, {"id": "SUP-ANDROID-CURRENT-001", "environment": {"android_api": "37", "os": "ubuntu-24.04", "system_image": "system-images;android-37.0;google_apis;x86_64", "system_image_revision": "6", "emulator_version": "37.2.12", "emulator_build": "16428233"}})
        identity = json.loads(self.identity.read_text())
        self.assertEqual(set(identity), {"format_version", "id", "serial", "sdk_root", "pid", "start_time", "executable", "avd", "socket_inodes", "metadata", "binary_version_line"})
        self.assertEqual(identity["start_time"], "123456")
        self.assertEqual(identity["pid"], 321)
        self.assertEqual(identity["socket_inodes"], [9001, 9002])
        self.assertEqual(set(identity["metadata"]), {str(path) for path in (self.config, self.runtime, self.image_xml, self.emulator_xml)})
        self.assertEqual(identity["executable"]["inode"], self.engine.stat().st_ino)
        self.assertEqual(identity["avd"]["system_image_path"], str(self.image / "system.img"))
        self.assertEqual(self.diagnostics.getvalue(), self.version.splitlines()[0] + "\n")
        self.assertEqual(self.command.call_count, 6)

    def test_minimum_and_installed_react_native(self) -> None:
        self.image_fixture("24", "27")
        self.api = "24\n"
        record = self.measure("SUP-ANDROID-MIN-001")
        self.assertEqual(record["environment"]["android_api"], "24")
        self.assertEqual(record["environment"]["system_image_revision"], "27")
        self.image_fixture("37.0", "6")
        self.api = "37\n"
        self.assertEqual(self.measure(probe.RN_ANDROID_CELL, react_native_app=self.app)["environment"]["react_native"], "0.83.5")
        self.version = self.version.replace("37.2.12.0", "37.2.12")
        self.assertEqual(self.measure()["environment"]["emulator_version"], "37.2.12")

    def test_minimum_react_native_requires_exact_installed_runtime(self) -> None:
        package = self.app / "node_modules/react-native/package.json"
        for version in ("0.82.0", "0.82.2", "0.83.10", None):
            with self.subTest(version=version):
                if version is None:
                    package.unlink()
                else:
                    package.write_text(json.dumps({"version": version}), encoding="utf-8")
                self.rejected(cell="SUP-RN-ANDROID-MIN-001", react_native_app=self.app)
        package.write_text('{"version":"0.82.1"}', encoding="utf-8")
        record = self.measure("SUP-RN-ANDROID-MIN-001", react_native_app=self.app)
        self.assertEqual(record, {"id": "SUP-RN-ANDROID-MIN-001", "environment": {
            "android_api": "37", "os": "ubuntu-24.04", "system_image": "system-images;android-37.0;google_apis;x86_64",
            "system_image_revision": "6", "emulator_version": "37.2.12", "emulator_build": "16428233", "react_native": "0.82.1",
        }})
        self.assertEqual(json.loads(self.identity.read_text())["id"], "SUP-RN-ANDROID-MIN-001")

    def test_wrong_minimum_cells_and_missing_app_reject_before_commands(self) -> None:
        for cell, app in (("SUP-RN-IOS-MIN-001", self.app), ("SUP-RN-ANDROID-MIN-002", self.app),
                          ("SUP-RN-ANDROID-MIN-001", None)):
            with self.subTest(cell=cell, app=app):
                self.command.reset_mock()
                self.rejected(cell=cell, react_native_app=app)
                self.command.assert_not_called()

    def test_inherited_conflicts_and_malformed_serials_precede_commands(self) -> None:
        for values in ({"ANDROID_SERIAL": "emulator-5554", "KOTLIN_ANDROID_SERIAL": "emulator-5556"}, {"ANDROID_SERIAL": " emulator-5554"}, {"KOTLIN_ANDROID_SERIAL": "emulator-05554"}, {"ANDROID_SERIAL": "emulator-65536"}, {"ANDROID_SERIAL": "emulator-5554\n"}, {"ANDROID_SERIAL": "physical"}, {"ANDROID_SERIAL": "emulator-5556"}):
            with self.subTest(values=values), mock.patch.dict(os.environ, values):
                self.rejected()
                self.command.assert_not_called()
        with mock.patch.dict(os.environ, {"ANDROID_SERIAL": "emulator-5554", "KOTLIN_ANDROID_SERIAL": "emulator-5554"}):
            self.measure()

    def test_unique_and_explicit_resolution_cli(self) -> None:
        out = io.StringIO()
        with mock.patch.object(sys, "argv", ["probe", "resolve-android-serial", "--sdk-root", str(self.sdk)]), contextlib.redirect_stdout(out):
            self.assertEqual(probe.main(), 0)
        self.assertEqual(out.getvalue(), "emulator-5554\n")
        self.devices += "physical-device device product:other\n127.0.0.1:5555 device product:network\n"
        with self.assertRaises(probe.ProbeError):
            probe.resolve_android_serial(self.sdk)
        self.measure()
        self.assertEqual(probe.resolve_android_serial(self.sdk, "emulator-5554"), "emulator-5554")
        with mock.patch.dict(os.environ, {"KOTLIN_ANDROID_SERIAL": "emulator-5554"}):
            self.assertEqual(probe.resolve_android_serial(self.sdk), "emulator-5554")

    def test_serial_change_is_rejected_before_the_next_device_command(self) -> None:
        def changed(command: list[str], **options: object) -> subprocess.CompletedProcess:
            result = self.execute(command, **options)
            os.environ["ANDROID_SERIAL"] = "emulator-5556"
            return result
        self.command.side_effect = changed
        self.rejected()
        self.assertEqual(self.command.call_count, 1)

    def test_hostile_inherited_adb_endpoints_cannot_change_local_measurement(self) -> None:
        endpoints = {"ADB_SERVER_SOCKET": "tcp:remote.example:9999", "ANDROID_ADB_SERVER_ADDRESS": "remote.example", "ANDROID_ADB_SERVER_PORT": "9999"}
        for inherited in (*({name: value} for name, value in endpoints.items()), endpoints):
            with self.subTest(inherited=inherited), mock.patch.dict(os.environ, inherited):
                self.command.reset_mock()
                self.assertEqual(probe.resolve_android_serial(self.sdk), "emulator-5554")
                self.assertEqual(self.measure()["environment"]["android_api"], "37")
                adb_commands = [call.args[0] for call in self.command.call_args_list if call.args[0][0] == str(self.adb)]
                self.assertEqual(len(adb_commands), 6)
                for command in adb_commands:
                    self.assertEqual(command[:3], [str(self.adb), "-L", "tcp:127.0.0.1:5037"])
                self.assertEqual({name: os.environ[name] for name in inherited}, inherited)
                self.command.reset_mock()
                with mock.patch.dict(os.environ, {"ANDROID_SERIAL": "emulator-5554", "KOTLIN_ANDROID_SERIAL": "emulator-5556"}):
                    self.rejected()
                    self.command.assert_not_called()

    def test_offline_missing_duplicate_and_malformed_devices(self) -> None:
        valid = self.devices
        for devices in ("List of devices attached\n", valid.replace(" device ", " offline "), valid.replace("emulator-5554", "emulator-5556"), valid + "emulator-5554 device\n", "List of devices attached\nmalformed\n", valid.replace("transport_id:1", "badfield"), valid.replace("emulator-5554", "emulator-bad"), "arbitrary output\n"):
            with self.subTest(devices=devices):
                self.devices = devices
                self.rejected()
        self.devices = valid
        self.command.reset_mock()
        def changed(command: list[str], **options: object) -> subprocess.CompletedProcess:
            if self.command.call_count == 6:
                self.devices = valid.replace(" device ", " offline ")
            return self.execute(command, **options)
        self.command.side_effect = changed
        self.rejected()

    def test_missing_duplicate_and_wrong_user_socket_owners(self) -> None:
        for fd in (self.proc / "321/fd").iterdir():
            fd.unlink()
        self.rejected()
        self.process(321)
        self.process(322)
        self.rejected()
        for fd in (self.proc / "322/fd").iterdir():
            fd.unlink()
        with mock.patch.object(probe.os, "getuid", return_value=os.getuid() + 1):
            self.rejected()

    def test_deleted_unrelated_replaced_and_unreadable_executable(self) -> None:
        exe = self.proc / "321/exe"
        exe.unlink()
        exe.symlink_to(str(self.engine) + " (deleted)")
        self.rejected()
        exe.unlink()
        outside = self.root / "unrelated"
        outside.write_text("other executable", encoding="utf-8")
        exe.symlink_to(outside)
        self.rejected()
        exe.unlink()
        exe.symlink_to(self.engine)
        original_stat = Path.stat
        def replaced(path: Path, *args: object, **options: object) -> os.stat_result:
            return original_stat(outside if path == exe else path, *args, **options)
        with mock.patch.object(Path, "stat", replaced):
            self.rejected()
        original_read = Path.read_text
        def unreadable(path: Path, *args: object, **options: object) -> str:
            if path == self.proc / "321/stat":
                raise PermissionError("secret arbitrary metadata")
            return original_read(path, *args, **options)
        with mock.patch.object(Path, "read_text", unreadable):
            self.rejected()
        self.assertNotIn("secret", self.diagnostics.getvalue())

    def test_unreadable_owner_and_unrelated_disappearance(self) -> None:
        unrelated = self.proc / "999"
        unrelated.mkdir()
        original = Path.iterdir
        def disappearing(path: Path):
            if path == unrelated / "fd":
                raise FileNotFoundError()
            return original(path)
        with mock.patch.object(Path, "iterdir", disappearing):
            self.measure()
        command = ["sudo", "--non-interactive", "ls", "-1", "--", str(self.proc / "321/fd")]
        def denied(arguments: list[str], **options: object) -> subprocess.CompletedProcess:
            if arguments == command:
                self.assertEqual(options, {"capture_output": True, "text": False, "check": True, "timeout": 30})
                raise subprocess.CalledProcessError(1, command, output=b"private output", stderr=b"private error")
            return self.execute(arguments, **options)
        self.command.side_effect = denied
        def unreadable(path: Path):
            if path == self.proc / "321/fd":
                raise PermissionError()
            return original(path)
        with mock.patch.object(Path, "iterdir", unreadable):
            self.rejected()
        self.assertTrue(any(call.args[0] == command for call in self.command.call_args_list))
        self.assertNotIn("private", self.diagnostics.getvalue())

    def test_restricted_directories_are_inspected_empty_valid_and_duplicate_owner_rejects(self) -> None:
        directory = self.proc / "321/fd"
        unrelated = self.proc / "999/fd"
        unrelated.mkdir(parents=True)
        descriptor = unrelated / "0"
        descriptor.symlink_to("/dev/null")
        restricted = {directory, unrelated}
        original = Path.iterdir

        def unreadable(path: Path):
            if path in restricted:
                raise PermissionError("private directory details")
            return original(path)

        def execute(command: list[str], **options: object) -> subprocess.CompletedProcess:
            if command[:5] == ["sudo", "--non-interactive", "ls", "-1", "--"]:
                self.assertEqual(len(command), 6)
                self.assertIn(Path(command[5]), restricted)
                self.assertEqual(options, {"capture_output": True, "text": False, "check": True, "timeout": 30})
                names = [fd.name for fd in original(Path(command[5]))]
                text = "".join(name + "\n" for name in names).encode("utf-8")
                return subprocess.CompletedProcess(command, 0, text, b"private tool diagnostics")
            return self.execute(command, **options)

        self.command.side_effect = execute
        with mock.patch.object(Path, "iterdir", unreadable):
            self.measure()
            command = ["sudo", "--non-interactive", "ls", "-1", "--", str(directory)]
            self.assertEqual(sum(call.args[0] == command for call in self.command.call_args_list), 2)
            self.assertEqual(json.loads(self.identity.read_text())["pid"], 321)
            descriptor.unlink()
            self.measure()
            self.assertEqual(json.loads(self.identity.read_text())["pid"], 321)
            duplicate = self.process(322)
            (duplicate / "fd/1").unlink()
            restricted.add(duplicate / "fd")
            self.rejected()
        self.assertNotIn("private", self.diagnostics.getvalue())

    def test_restricted_directory_denial_timeout_and_malformed_names_reject(self) -> None:
        directory = self.proc / "321/fd"
        command = ["sudo", "--non-interactive", "ls", "-1", "--", str(directory)]
        original = Path.iterdir
        self.output.write_text("previous environment", encoding="utf-8")
        self.identity.write_text("previous identity", encoding="utf-8")

        def unreadable(path: Path):
            if path == directory:
                raise PermissionError("private directory details")
            return original(path)

        failures = [
            subprocess.CalledProcessError(1, command, output=b"private output", stderr=b"private error"),
            subprocess.TimeoutExpired(command, 30, output=b"private output", stderr=b"private error"),
            b"\n", b"0", b"0\n\n", b"0\r\n", b"0\r", b"0\n1", b"00\n", b"01\n", b"-1\n", b"+1\n",
            b" 0\n", b"0 \n", b"0\t\n", b"0\x00\n", b"\xff\n", "١\n".encode("utf-8"),
            b"../0\n", b"/0\n", b"0/1\n", b"0\\1\n", b"0\n0\n", b"0\n1\n0\n",
        ]
        for failure in failures:
            with self.subTest(failure=failure):
                def execute(arguments: list[str], **options: object) -> subprocess.CompletedProcess:
                    if arguments == command:
                        self.assertEqual(options, {"capture_output": True, "text": False, "check": True, "timeout": 30})
                        if isinstance(failure, Exception):
                            raise failure
                        return subprocess.CompletedProcess(arguments, 0, failure, b"private tool diagnostics")
                    return self.execute(arguments, **options)
                self.command.side_effect = execute
                with mock.patch.object(Path, "iterdir", unreadable), mock.patch.object(probe.os, "readlink") as readlink:
                    self.rejected()
                    readlink.assert_not_called()
        self.assertNotIn("private", self.diagnostics.getvalue())

    def test_restricted_descriptors_are_inspected_and_duplicate_owner_rejects(self) -> None:
        descriptor = self.proc / "999/fd/0"
        descriptor.parent.mkdir(parents=True)
        descriptor.symlink_to("/dev/null")
        restricted = {descriptor}
        original = os.readlink

        def unreadable(path: Path, *args: object, **options: object) -> str:
            if Path(path) in restricted:
                raise PermissionError("private descriptor details")
            return original(path, *args, **options)

        def execute(command: list[str], **options: object) -> subprocess.CompletedProcess:
            if command[:4] == ["sudo", "--non-interactive", "readlink", "--"]:
                self.assertEqual(len(command), 5)
                self.assertIn(Path(command[4]), restricted)
                self.assertEqual(options, {"capture_output": True, "text": False, "check": True, "timeout": 30})
                return subprocess.CompletedProcess(command, 0, original(command[4]).encode("utf-8") + b"\n", b"private tool diagnostics")
            return self.execute(command, **options)

        self.command.side_effect = execute
        with mock.patch.object(probe.os, "readlink", unreadable):
            self.measure()
            command = ["sudo", "--non-interactive", "readlink", "--", str(descriptor)]
            self.assertEqual(sum(call.args[0] == command for call in self.command.call_args_list), 2)
            self.assertEqual(json.loads(self.identity.read_text())["pid"], 321)
            duplicate = self.process(322)
            (duplicate / "fd/1").unlink()
            restricted.add(duplicate / "fd/0")
            self.rejected()
        self.assertNotIn("private", self.diagnostics.getvalue())

    def test_restricted_descriptor_denial_timeout_and_malformed_text_reject(self) -> None:
        descriptor = self.proc / "321/fd/0"
        command = ["sudo", "--non-interactive", "readlink", "--", str(descriptor)]
        original = os.readlink
        self.output.write_text("previous environment", encoding="utf-8")
        self.identity.write_text("previous identity", encoding="utf-8")

        def unreadable(path: Path, *args: object, **options: object) -> str:
            if Path(path) == descriptor:
                raise PermissionError("private descriptor details")
            return original(path, *args, **options)

        failures = [
            subprocess.CalledProcessError(1, command, output=b"private output", stderr=b"private error"),
            subprocess.TimeoutExpired(command, 30, output=b"private output", stderr=b"private error"),
            b"", b"\n", b"/dev/null", b"/dev/null\n\n", b"/dev/null\nother\n", b"/dev/null\r\n", b"/dev/null\r",
            b"/dev/\x00null\n", b"/dev/\tnull\n", b"/dev/\x7fnull\n", b"/dev/\xc2\x85null\n", b"/dev/\xffnull\n",
        ]
        for failure in failures:
            with self.subTest(failure=failure):
                def execute(arguments: list[str], **options: object) -> subprocess.CompletedProcess:
                    if arguments == command:
                        self.assertEqual(options, {"capture_output": True, "text": False, "check": True, "timeout": 30})
                        if isinstance(failure, Exception):
                            raise failure
                        return subprocess.CompletedProcess(arguments, 0, failure, b"private tool diagnostics")
                    return self.execute(arguments, **options)
                self.command.side_effect = execute
                with mock.patch.object(probe.os, "readlink", unreadable):
                    self.rejected()
        self.assertNotIn("private", self.diagnostics.getvalue())

    def test_wrong_duplicate_missing_avd_selectors_and_stat(self) -> None:
        for selectors in (["-avd", "Wrong"], ["-avd", self.name, "@" + self.name], ["-avd", self.name, "-avd", self.name], [], ["-avd"]):
            with self.subTest(selectors=selectors):
                self.process(321, selectors=selectors)
                self.rejected()
        self.process(321, selectors=["@" + self.name])
        self.measure()
        self.output.unlink()
        self.identity.unlink()
        (self.proc / "321/stat").write_text("321 (qemu) S malformed\n", encoding="utf-8")
        self.rejected()

    def test_console_api_and_host_metadata_fail_closed(self) -> None:
        for attribute, values in (("console_name", [self.name + "\n", self.name + "\nOK\nextra\n", "\nOK\n", "Wrong\nOK\n"]), ("console_path", ["/missing/avd\nOK\n", str(self.avd) + "\nKO\n"]), ("api", ["37\nextra\n", "037\n", "24\n", "\n"])):
            original = getattr(self, attribute)
            for value in values:
                with self.subTest(attribute=attribute, value=value):
                    setattr(self, attribute, value)
                    self.rejected()
            setattr(self, attribute, original)
        for function, value in (("system", "Darwin"), ("machine", "aarch64"), ("freedesktop_os_release", {"ID": "ubuntu", "VERSION_ID": "22.04"}), ("freedesktop_os_release", None)):
            with mock.patch.object(probe.platform, function, return_value=value):
                self.command.reset_mock()
                self.rejected()
                self.command.assert_not_called()

    def test_wrong_runtime_image_escape_and_duplicate_ini(self) -> None:
        config, runtime = self.config.read_text(), self.runtime.read_text()
        for path, text in ((self.config, config + "image.sysdir.1=other\n"), (self.runtime, runtime + "avd.name=other\n"), (self.runtime, runtime.replace(self.name, "Wrong")), (self.config, config.replace(str(self.image.relative_to(self.sdk)), str(self.avd))), (self.runtime, runtime.replace(str(self.image / "system.img"), str(self.engine))), (self.config, "malformed\n")):
            with self.subTest(path=path, text=text):
                path.write_text(text, encoding="utf-8")
                self.rejected()
                self.config.write_text(config, encoding="utf-8")
                self.runtime.write_text(runtime, encoding="utf-8")

    def test_wrong_duplicate_malformed_and_preview_packages(self) -> None:
        for path in (self.image_xml, self.emulator_xml):
            original = path.read_text()
            variants = ["malformed", original.replace("<revision>", "<revision><major>9</major>"), original.replace("<localPackage", '<localPackage path="wrong"><revision><major>1</major></revision></localPackage><localPackage', 1)]
            missing_components = []
            if path == self.image_xml:
                variants += [original.replace("<major>6</major>", "<major>0</major>"), original.replace("</revision>", "<minor>1</minor></revision>"), original.replace("</revision>", "<micro>1</micro></revision>"), original.replace("</revision>", "<preview>1</preview></revision>"), original.replace("x86_64", "arm64-v8a")]
            else:
                missing_components = [original.replace(component, "") for component in ("<major>37</major>", "<minor>2</minor>", "<micro>12</micro>")]
                variants += missing_components
                variants += [original.replace("</revision>", "<preview>0</preview></revision>"), original.replace("<micro>12</micro>", "<micro>13</micro>")]
            for variant in variants:
                with self.subTest(path=path, variant=variant):
                    self.command.reset_mock()
                    path.write_text(variant, encoding="utf-8")
                    self.rejected()
                    if variant in missing_components:
                        self.assertEqual(self.command.call_count, 4)
            path.write_text(original, encoding="utf-8")

    def test_binary_versions_and_installed_react_native_mismatch(self) -> None:
        original = self.version
        for version in (original.replace("37.2.12.0", "37.2.12.1"), original.replace("37.2.12.0", "37.2.12.0.0"), original.replace("37.2.12.0", "37.2.12-preview"), original.replace("37.2.12.0", "37.2.13"), original.replace("16428233", "0"), original + original, "arbitrary output\n"):
            with self.subTest(version=version):
                self.version = version
                self.rejected()
        self.version = original
        package = self.app / "node_modules/react-native/package.json"
        for text in ('{"version":"0.84.0"}', '{"version":"0.82.1"}', None):
            with self.subTest(metadata=text):
                if text is None:
                    package.unlink()
                else:
                    package.write_text(text, encoding="utf-8")
                self.rejected(cell=probe.RN_ANDROID_CELL, react_native_app=self.app)

    def test_resume_matches_complete_records_and_rejects_every_identity_change(self) -> None:
        options = self.initial()
        self.measure(**options)
        initial = json.loads(options["initial_identity"].read_text())
        for field, value in (("pid", 322), ("start_time", "654321"), ("socket_inodes", [9001]), ("format_version", True), ("binary_version_line", "different"), ("serial", "emulator-5556")):
            with self.subTest(field=field):
                changed = copy.deepcopy(initial)
                changed[field] = value
                options["initial_identity"].write_text(json.dumps(changed), encoding="utf-8")
                self.rejected(**options)
        for field in ("executable", "metadata", "avd"):
            with self.subTest(field=field):
                changed = copy.deepcopy(initial)
                if field == "metadata":
                    changed[field][str(self.config)]["sha256"] = "0" * 64
                elif field == "executable":
                    changed[field]["inode"] += 1
                else:
                    changed[field]["name"] = "other"
                options["initial_identity"].write_text(json.dumps(changed), encoding="utf-8")
                self.rejected(**options)
        options["initial_identity"].write_text(json.dumps(initial), encoding="utf-8")
        previous = json.loads(options["initial_environment"].read_text())
        previous["environment"]["emulator_build"] = "123"
        options["initial_environment"].write_text(json.dumps(previous), encoding="utf-8")
        self.rejected(**options)

    def test_resume_rejects_actual_changed_pid_start_files_sockets_and_guest(self) -> None:
        options = self.initial()
        for path in (self.config, self.runtime, self.image_xml, self.emulator_xml):
            stat = path.stat()
            original = path.read_bytes()
            with self.subTest(path=path):
                path.write_bytes(original + b"\n")
                self.rejected(**options)
            path.write_bytes(original)
            os.utime(path, ns=(stat.st_atime_ns, stat.st_mtime_ns))
        engine_stat, original_engine = self.engine.stat(), self.engine.read_bytes()
        self.engine.write_bytes(original_engine + b"replacement")
        self.rejected(**options)
        self.engine.write_bytes(original_engine)
        os.utime(self.engine, ns=(engine_stat.st_atime_ns, engine_stat.st_mtime_ns))
        self.process(321, start="99999")
        self.rejected(**options)
        self.process(321)
        for fd in (self.proc / "321/fd").iterdir():
            fd.unlink()
        self.process(322)
        self.rejected(**options)
        for fd in (self.proc / "322/fd").iterdir():
            fd.unlink()
        self.process(321)
        self.sockets((9001, 9003))
        (self.proc / "321/fd/1").unlink()
        (self.proc / "321/fd/1").symlink_to("socket:[9003]")
        self.rejected(**options)
        self.sockets()
        self.process(321)
        self.image_fixture("38.0", "1")
        self.api = "38\n"
        self.rejected(**options)

    def test_rechecks_mutations_during_version_probe(self) -> None:
        def mutation(command: list[str], **options: object) -> subprocess.CompletedProcess:
            result = self.execute(command, **options)
            if command[-1] == "-version":
                self.config.write_text(self.config.read_text() + "# changed\n", encoding="utf-8")
            return result
        self.command.side_effect = mutation
        self.rejected()
        def process_mutation(command: list[str], **options: object) -> subprocess.CompletedProcess:
            result = self.execute(command, **options)
            if command[-1] == "-version":
                self.process(321, start="777")
            return result
        self.command.side_effect = process_mutation
        self.rejected()
        self.process(321)
        def socket_mutation(command: list[str], **options: object) -> subprocess.CompletedProcess:
            result = self.execute(command, **options)
            if command[-1] == "-version":
                self.sockets((9001, 9003))
            return result
        self.command.side_effect = socket_mutation
        self.rejected()

    def test_resume_duplicate_missing_unknown_records_and_paired_arguments(self) -> None:
        options = self.initial()
        identity, environment = options["initial_identity"].read_text(), options["initial_environment"].read_text()
        for path, text in ((options["initial_identity"], '{"format_version":1,"format_version":1}'), (options["initial_identity"], "{}"), (options["initial_environment"], '{"id":"a","id":"b"}'), (options["initial_environment"], '{"id":"SUP-ANDROID-CURRENT-001","environment":{},"extra":1}')):
            with self.subTest(text=text):
                path.write_text(text, encoding="utf-8")
                self.rejected(**options)
            options["initial_identity"].write_text(identity, encoding="utf-8")
            options["initial_environment"].write_text(environment, encoding="utf-8")
        self.rejected(initial_identity=options["initial_identity"])
        self.rejected(initial_environment=options["initial_environment"])
        self.rejected(react_native_app=self.app)
        self.rejected(cell=probe.RN_ANDROID_CELL)

    def test_all_commands_are_bounded_and_output_write_failure_is_atomic(self) -> None:
        for index in range(1, 7):
            self.command.reset_mock()
            def timeout(command: list[str], **options: object) -> subprocess.CompletedProcess:
                if self.command.call_count == index:
                    raise subprocess.TimeoutExpired(command, 30, output="secret output")
                return self.execute(command, **options)
            self.command.side_effect = timeout
            self.rejected()
            self.assertEqual(self.command.call_count, index)
        self.command.side_effect = self.execute
        self.output.write_text("previous environment", encoding="utf-8")
        self.identity.write_text("previous identity", encoding="utf-8")
        with mock.patch.object(probe.os, "replace", side_effect=OSError("secret")):
            self.rejected()
        self.assertEqual(list(self.root.glob(".identity.json.*")), [])
        self.assertNotIn("secret", self.diagnostics.getvalue())

    def test_sdk_command_errors_and_invalid_serial_leave_outputs_absent(self) -> None:
        for serial in ("", "emulator-0", "emulator-5554 extra", "physical-device"):
            with self.subTest(serial=serial), self.assertRaises(probe.ProbeError):
                probe.probe_android("SUP-ANDROID-CURRENT-001", self.sdk, serial, self.output, self.identity)
            self.command.assert_not_called()
        self.adb.chmod(0o644)
        self.rejected()
        self.command.assert_not_called()
        self.adb.chmod(0o755)
        for error in (OSError("secret"), subprocess.CalledProcessError(1, ["adb"], output="secret"), subprocess.CompletedProcess(["adb"], 0, None, "")):
            self.command.side_effect = error if isinstance(error, BaseException) else None
            self.command.return_value = error
            self.rejected()
        self.assertEqual(self.diagnostics.getvalue(), "")

    def test_android_cli_required_arguments_and_failure_status(self) -> None:
        arguments = ["probe", "android", "--cell", "SUP-ANDROID-CURRENT-001", "--sdk-root", str(self.sdk), "--serial", "emulator-5554", "--output", str(self.output), "--identity-output", str(self.identity)]
        with mock.patch.object(sys, "argv", arguments):
            self.assertEqual(probe.main(), 0)
        for missing in ("--cell", "--sdk-root", "--serial", "--output", "--identity-output"):
            index = arguments.index(missing)
            self.command.reset_mock()
            with self.subTest(missing=missing), mock.patch.object(sys, "argv", arguments[:index] + arguments[index + 2:]), self.assertRaises(SystemExit) as raised:
                probe.main()
            self.assertEqual(raised.exception.code, 2)
            self.command.assert_not_called()
        with mock.patch.object(sys, "argv", arguments), mock.patch.dict(os.environ, {"ANDROID_SERIAL": "emulator-5556"}):
            self.assertEqual(probe.main(), 1)
        self.command.assert_not_called()


if __name__ == "__main__":
    unittest.main()
