from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import time


MAX_ATTEMPTS = 3
DEADLINE_SECONDS = 120
RETRY_DELAYS = (2, 4)

HARD_FAILURES = (
    ("certificate", r"x509:|certificate (?:verify|verification|signed|expired|invalid)|unknown authority|tls: failed to verify"),
    ("checksum", r"checksum mismatch|security error:|invalid checksum"),
    ("authentication", r"\b(?:401|403)\b|unauthorized|forbidden|authentication required|permission denied|could not read username"),
    ("resolution", r"\b(?:404|410)\b|unknown revision|invalid version|module .* not found|no matching versions|unrecognized import path|ambiguous import|missing go\.sum entry"),
)
TRANSIENT_FAILURES = (
    ("rate_limit", r"\b429\b|too many requests"),
    ("service", r"\b(?:502|503|504)\b|bad gateway|service unavailable|gateway timeout"),
    ("dns", r"temporary failure in name resolution|server misbehaving|dns.*timeout|lookup .*:.*(?:timeout|temporary|connection refused)"),
    ("tls_timeout", r"tls handshake (?:timeout|timed out)"),
    ("connection", r"connection (?:reset|refused|timed out|timeout)|network is unreachable|i/o timeout|operation timed out|context deadline exceeded|client\.timeout exceeded"),
)


def classify_failure(output: str) -> tuple[str, bool]:
    for category, pattern in HARD_FAILURES:
        if re.search(pattern, output, re.IGNORECASE):
            return category, False
    for category, pattern in TRANSIENT_FAILURES:
        if re.search(pattern, output, re.IGNORECASE):
            return category, True
    return "unknown", False


def classify_result(stdout: str, stderr: str) -> tuple[str, bool, bool]:
    errors = []
    decoder = json.JSONDecoder()
    position = 0
    while position < len(stdout):
        while position < len(stdout) and stdout[position].isspace():
            position += 1
        if position == len(stdout):
            break
        try:
            item, position = decoder.raw_decode(stdout, position)
        except json.JSONDecodeError:
            errors.append(stdout[position:])
            break
        if isinstance(item, dict) and isinstance(item.get("Error"), str):
            errors.append(item["Error"])
    errors.extend(line for line in stderr.splitlines() if line.strip()
                  and not re.fullmatch(r"go: downloading \S+ \S+", line))
    categories = [classify_failure(error) for error in errors]
    for category, retryable in categories:
        if not retryable:
            return category, False, True
    if categories:
        category = categories[0][0] if len(set(categories)) == 1 else "transient"
        return category, True, True
    return "unknown", False, False


def lockfiles_changed(original: dict[Path, bytes]) -> bool:
    for path, content in original.items():
        try:
            current = path.read_bytes()
        except OSError:
            return True
        if current != content:
            return True
    return False


def download(module_dir: Path) -> int:
    if not module_dir.is_dir() or any(
        not (module_dir / name).is_file() or (module_dir / name).is_symlink()
        for name in ("go.mod", "go.sum")
    ):
        print("Go module acquisition failed: invalid_module_directory", file=sys.stderr)
        return 1

    try:
        original = {module_dir / name: (module_dir / name).read_bytes() for name in ("go.mod", "go.sum")}
    except OSError:
        print("Go module acquisition failed: lockfile_unreadable", file=sys.stderr)
        return 1
    environment = os.environ.copy()
    environment["GOFLAGS"] = ""
    environment["GOWORK"] = "off"
    deadline = time.monotonic() + DEADLINE_SECONDS

    with tempfile.TemporaryDirectory(prefix="synchro-go-download-") as temporary:
        alternate = {Path(temporary) / path.name: content for path, content in original.items()}
        for path, content in alternate.items():
            path.write_bytes(content)
        for attempt in range(1, MAX_ATTEMPTS + 1):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                print(f"Go module acquisition attempt {attempt}: deadline", file=sys.stderr)
                return 1
            try:
                result = subprocess.run(
                    ["go", "mod", "download", "-json", f"-modfile={temporary}/go.mod"],
                    cwd=module_dir,
                    env=environment,
                    capture_output=True,
                    text=True,
                    errors="replace",
                    timeout=remaining,
                    check=False,
                )
                category, retryable, reported_error = classify_result(result.stdout, result.stderr)
            except subprocess.TimeoutExpired:
                category, retryable = "deadline", False
                result = None
                reported_error = True
            except OSError:
                category, retryable = "execution", False
                result = None
                reported_error = True

            if lockfiles_changed(original) or lockfiles_changed(alternate):
                category, retryable = "lockfile_changed", False
            if result is not None and result.returncode == 0 and not reported_error and category != "lockfile_changed":
                print(f"Go module acquisition succeeded on attempt {attempt}")
                return 0

            print(f"Go module acquisition attempt {attempt}/{MAX_ATTEMPTS} failed: {category}", file=sys.stderr)
            if not retryable or attempt == MAX_ATTEMPTS:
                return 1
            delay = min(RETRY_DELAYS[attempt - 1], max(0, deadline - time.monotonic()))
            if delay <= 0:
                return 1
            time.sleep(delay)
    return 1


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--module-dir", required=True, type=Path)
    args = parser.parse_args()
    return download(args.module_dir.resolve())


if __name__ == "__main__":
    raise SystemExit(main())
