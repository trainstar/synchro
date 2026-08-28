#!/usr/bin/env python3
"""Run one CI gate and write its terminal structured result."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path


COUNT_PATTERNS = (
    re.compile(r"testresult:\s+(\d+)\s+tests? passed\b"),
    re.compile(r"testresult:\s+(\d+)\s+Rust tests passed\b"),
)
TARGET_PASS = re.compile(r"testresult:\s+target_pass\b")


def parse_count(output: str) -> int:
    counts = [int(match.group(1)) for pattern in COUNT_PATTERNS for match in pattern.finditer(output)]
    if counts:
        return sum(counts)
    if TARGET_PASS.search(output):
        return 1
    return 0


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
