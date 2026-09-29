#!/usr/bin/env python3
"""Print one "version url sha256" line for each released update origin after the baseline.

Usage: update-origins.py <update-origins.json> <update-baseline.json>
"""
import json
import re
import sys

VERSION = re.compile(r"^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(?:-rc\.([1-9][0-9]*))?$")
DIGEST = re.compile(r"^[0-9a-f]{64}$")


def reject_duplicates(pairs):
    keys = [key for key, _ in pairs]
    if len(keys) != len(set(keys)):
        raise ValueError("duplicate key")
    return dict(pairs)


def load(path):
    with open(path, encoding="utf-8") as handle:
        return json.load(handle, object_pairs_hook=reject_duplicates)


def version_key(version):
    # A release follows each of its release candidates.
    major, minor, patch, candidate = VERSION.match(version).groups()
    return (int(major), int(minor), int(patch), 0 if candidate else 1, int(candidate or 0))


def main():
    if len(sys.argv) != 3:
        raise SystemExit(__doc__.strip())
    origins = load(sys.argv[1])
    baseline = load(sys.argv[2])
    if not isinstance(origins, dict) or set(origins) != {"origins"} or not isinstance(origins["origins"], list) or not origins["origins"]:
        raise SystemExit("update origins must be one object with a nonempty origins list")
    previous = baseline.get("version")
    if not isinstance(previous, str) or not VERSION.match(previous):
        raise SystemExit("update baseline version is invalid")
    for origin in origins["origins"]:
        if not isinstance(origin, dict) or set(origin) != {"version", "artifact_sha256"}:
            raise SystemExit("each update origin must contain only version and artifact_sha256")
        version, digest = origin["version"], origin["artifact_sha256"]
        if not isinstance(version, str) or not VERSION.match(version) or version_key(version) <= version_key(previous):
            raise SystemExit("update origin versions must be X.Y.Z or X.Y.Z-rc.N and ascend after the baseline")
        if not isinstance(digest, str) or not DIGEST.match(digest):
            raise SystemExit("update origin artifact_sha256 must be 64 lowercase hexadecimal digits")
        url = f"https://github.com/trainstar/synchro/releases/download/v{version}/synchro-pg-pg18-ubuntu24.04-linux-x64-{version}.tar.gz"
        print(version, url, digest)
        previous = version


if __name__ == "__main__":
    main()
