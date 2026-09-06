#!/usr/bin/env python3
"""Run one CI gate and write its terminal structured result."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
from pathlib import Path


COUNT_PATTERNS = (
    re.compile(r"testresult:\s+(\d+)\s+tests? passed\b"),
    re.compile(r"testresult:\s+(\d+)\s+Rust tests passed\b"),
)
TARGET_PASS = re.compile(r"testresult:\s+target_pass\b")
GATE_VARIABLES = (
    "BLACKBOX_TEST_COUNT",
    "DETOX_ARGS",
    "GO_TEST_ARGS",
    "GO_TEST_PKGS",
    "GRADLE_TEST_ARGS",
    "KOTLIN_ANDROID_SERIAL",
    "MUTATION_CONTROL_EXPECT",
    "MUTATION_CONTROL_TEST",
    "PGRX_TEST_NAME",
    "RN_ANDROID_DETOX_CONFIG",
    "SUPPORT_CELL_ID",
    "SUPPORT_PLATFORM_VERSION",
    "TESTRESULT_TEST_NAME",
)
# Free-form argument variables can carry credentials. Store only their digests.
DIGESTED_GATE_VARIABLES = {"DETOX_ARGS", "GO_TEST_ARGS", "GRADLE_TEST_ARGS"}
GATE_VARIABLE_DEFAULTS = {
    "BLACKBOX_TEST_COUNT": "1",
    "DETOX_ARGS": "",
    "GO_TEST_ARGS": "-v -count=1 -p 1",
    "GO_TEST_PKGS": "./...",
    "GRADLE_TEST_ARGS": "--rerun-tasks",
    "MUTATION_CONTROL_EXPECT": "target_pass",
    "MUTATION_CONTROL_TEST": "",
    "PGRX_TEST_NAME": "",
    "RN_ANDROID_DETOX_CONFIG": "android.emu.release",
    "SUPPORT_CELL_ID": "",
    "SUPPORT_PLATFORM_VERSION": "",
    "TESTRESULT_TEST_NAME": "",
}
EMBEDDED_CREDENTIAL_URL = re.compile(r"[a-z][a-z0-9+.-]*://[^/\s:@]+:[^/@\s]+@", re.IGNORECASE)
COMMIT = re.compile(r"^[0-9a-f]{40}$")


def parse_count(output: str) -> int:
    counts = [int(match.group(1)) for pattern in COUNT_PATTERNS for match in pattern.finditer(output)]
    if counts:
        return sum(counts)
    if TARGET_PASS.search(output):
        return 1
    return 0


def gate_variables(environment: dict[str, str], command: list[str] | None = None) -> list[dict[str, str]]:
    effective = dict(GATE_VARIABLE_DEFAULTS)
    effective["KOTLIN_ANDROID_SERIAL"] = environment.get("ANDROID_SERIAL", "")
    for name in GATE_VARIABLES:
        if name in environment:
            effective[name] = environment[name]
    for argument in command or []:
        name, separator, value = argument.partition("=")
        if separator and name in GATE_VARIABLES:
            effective[name] = value
    variables = []
    for name in GATE_VARIABLES:
        if name not in effective:
            raise ValueError(f"missing required gate variable: {name}")
        value = effective[name]
        if len(value) > 4096 or any(character in value for character in "\x00\r\n"):
            raise ValueError(f"gate variable {name} has an unsafe value")
        if name in DIGESTED_GATE_VARIABLES or EMBEDDED_CREDENTIAL_URL.search(value):
            value = "sha256:" + hashlib.sha256(value.encode("utf-8")).hexdigest()
        variables.append({"name": name, "value": value})
    return variables


def source_commit() -> str:
    try:
        commit = subprocess.check_output(
            ["git", "rev-parse", "--verify", "HEAD"],
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except (OSError, subprocess.CalledProcessError) as error:
        raise ValueError(f"cannot resolve the repository commit: {error}") from error
    if not COMMIT.fullmatch(commit):
        raise ValueError("repository HEAD is not a full commit hash")
    return commit


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gate", required=True, help="Make target name")
    parser.add_argument("--output", type=Path, required=True, help="JSON result path")
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if not re.fullmatch(r"[a-z0-9][a-z0-9-]*", args.gate):
        parser.error("gate must be a lowercase Make target")
    command = args.command
    if command[:1] == ["--"]:
        command = command[1:]
    if not command:
        parser.error("a gate command is required")
    try:
        variables = gate_variables(dict(os.environ), command)
        commit = source_commit()
    except ValueError as error:
        parser.error(str(error))

    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    output_parts: list[str] = []
    assert process.stdout is not None
    for line in process.stdout:
        sys.stdout.write(line)
        sys.stdout.flush()
        output_parts.append(line)
    status = process.wait()
    output = "".join(output_parts)
    test_count = parse_count(output)
    result = {
        "gate": args.gate,
        "status": "passed" if status == 0 and test_count > 0 else "failed",
        "terminal": status == 0 and test_count > 0,
        "test_count": test_count,
        "exit_code": status,
        "gate_variables": variables,
        "source_commit": commit,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    temporary = args.output.with_name(f".{args.output.name}.tmp")
    temporary.write_text(json.dumps(result, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(args.output)
    if status != 0:
        return status
    if test_count == 0:
        print(f"gate {args.gate} produced no structured passing test result", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
