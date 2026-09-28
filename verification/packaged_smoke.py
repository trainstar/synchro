#!/usr/bin/env python3
"""Generate fail-closed packaged smoke cell and summary evidence."""

from __future__ import annotations

import argparse
import base64
import hashlib
import hmac
import http.server
import json
import math
import os
import re
import secrets
import signal
import socket
import socketserver
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit


SMOKE_OPERATIONS = ("connect", "push", "pull", "kill", "resume")
CELL_SCHEMA_VERSION = 1
SUMMARY_SCHEMA_VERSION = 1
SHA256 = re.compile(r"^[0-9a-f]{64}$")
COMMIT = re.compile(r"^[0-9a-f]{40}$")
APP_RESULT_PATH = "/result"
APP_RESULT_MAX_BYTES = 4096
APP_RESULT_MAX_ERROR_LENGTH = 512
APP_RESULT_READ_TIMEOUT_SECONDS = 5.0
# The consumer references two rows of the training dataset authored seed
# (conformance/dataset/authored.go: OrgA and BackSquat). The fixture prepares
# that seed with CLIENT_DATASET=1.
DATASET_ORGANIZATION_ID = "00000001-0000-4000-8000-000000000001"
DATASET_EXERCISE_ID = "00000004-0000-4000-8000-000000000001"
DATASET_EXERCISE_NAME = "Back Squat"
# The installed consumer writes these rows through its public SQL path. The
# harness writes REMOTE_SETS and a new program title while the consumer is dead.
AUTHORED_PROGRAM = {
    "title": 'Packaged Block \u2705 "consumer"',
    "description": "",
    "visibility": "private",
    "week_count": 6,
    "settings": {"deload_week": 4},
}
AUTHORED_WORKOUT = {
    "name": "Packaged Heavy Day",
    "notes": 'Tiefe gut \u2705\nline "two"\\',
    "scheduled_on": "2026-09-14",
    "started_at": "2026-09-14T06:30:00.123456Z",
    "duration_seconds": 3725,
    # The server generates duration_minutes as duration_seconds / 60.
    "duration_minutes": 62,
    "external_ref": "9007199254740993",
}
AUTHORED_ENTRY = {"position": 1, "target": {"reps": [5, 5, 3], "sets": 3}}
AUTHORED_TIMESTAMP = "2026-09-14T12:00:00.000000Z"
DURABLE_TIMESTAMP = "2026-09-14T12:05:00.000000Z"
# Each set is (set_index, reps, weight_kg, rpe, duration_ms, completed_at, note).
CLIENT_SETS = (
    (1, 5, "100", "7.5", "1", "2026-09-14T06:40:00.000001Z", ""),
    (2, 5, "102.5", "8", "9007199254740993", "2026-09-14T06:45:00.000000Z", "solid"),
    (3, 3, "110.25", "8.5", None, None, '\u65b0\u8a18\u9332 \U0001f389 "PR"'),
)
# The offline update that the consumer queues before it is killed.
DURABLE_SET_INDEX = 3
DURABLE_REPS = 8
REMOTE_SETS = (
    (4, 2, "120", "9", None, None, "server \u00e9\u4e16"),
    (5, 1, "125.5", "9.5", None, None, ""),
)
# Hand-computed sums of reps * weight_kg. Only the server rollup trigger
# writes workouts.total_volume_kg, and only a pull delivers it to the client.
INITIAL_TOTAL_VOLUME_KG = "1343.25"  # 5 * 100 + 5 * 102.5 + 3 * 110.25
REMOTE_TOTAL_VOLUME_KG = "1708.75"  # 1343.25 + 2 * 120 + 1 * 125.5
FINAL_TOTAL_VOLUME_KG = "2260"  # 1708.75 + (8 - 3) * 110.25
INITIAL_OBSERVED_SETS = "1:5:100,2:5:102.5,3:8:110.25"
FINAL_OBSERVED_SETS = "1:5:100,2:5:102.5,3:8:110.25,4:2:120,5:1:125.5"
CLIENT_RESUMED_WRITE = {"set_index": DURABLE_SET_INDEX, "reps": DURABLE_REPS}
OBSERVED_FIELDS = ("exercise_name", "program_title", "total_volume_kg", "sets")
# The server consumer uploads this customer insert only after the adapter restarts.
SERVER_OFFLINE_WRITE = {"customer_id": "00000000-0000-4000-8000-000000000118", "customer_name": "Packaged server offline"}


class EvidenceError(ValueError):
    """Describe one packaged smoke evidence error."""


def load_json(path: Path, label: str) -> Any:
    try:
        with path.open(encoding="utf-8") as stream:
            return json.load(stream)
    except (OSError, json.JSONDecodeError) as error:
        raise EvidenceError(f"{label} is missing or malformed: {error}") from error


def write_json(path: Path, value: object, mode: int = 0o644) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, mode)
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            json.dump(value, stream, indent=2, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise


def source_commit(repo_root: Path) -> str:
    try:
        commit = subprocess.check_output(
            ["git", "-C", str(repo_root), "rev-parse", "--verify", "HEAD"],
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except (OSError, subprocess.CalledProcessError) as error:
        raise EvidenceError(f"cannot resolve the repository commit: {error}") from error
    if not COMMIT.fullmatch(commit):
        raise EvidenceError("repository HEAD is not a full commit hash")
    return commit


def required_cells(repo_root: Path) -> list[str]:
    matrix = load_json(repo_root / "conformance/support-matrix.json", "support matrix")
    if not isinstance(matrix, dict) or not isinstance(matrix.get("cells"), list):
        raise EvidenceError("support matrix does not declare cells")
    result: list[str] = []
    seen: set[str] = set()
    for raw_cell in matrix["cells"]:
        if not isinstance(raw_cell, dict):
            raise EvidenceError("support matrix contains a malformed cell")
        cell_id = raw_cell.get("id")
        if not isinstance(cell_id, str) or not cell_id:
            raise EvidenceError("support matrix contains a cell without an id")
        if cell_id in seen:
            raise EvidenceError(f"support matrix repeats cell {cell_id}")
        seen.add(cell_id)
        if raw_cell.get("policy") == "required":
            result.append(cell_id)
    if not result:
        raise EvidenceError("support matrix has no packaged smoke cells")
    return result


def hash_files(paths: list[Path]) -> list[str]:
    hashes: list[str] = []
    seen: set[str] = set()
    for path in paths:
        try:
            data = path.read_bytes()
        except OSError as error:
            raise EvidenceError(f"cannot read packaged artifact {path}: {error}") from error
        digest = hashlib.sha256(data).hexdigest()
        if digest not in seen:
            hashes.append(digest)
            seen.add(digest)
    return hashes


def operation_entries(status: str, test_count: int) -> list[dict[str, object]]:
    return [
        {
            "name": operation,
            "status": status,
            "terminal": True,
            "test_count": test_count,
        }
        for operation in SMOKE_OPERATIONS
    ]


def begin_cell(repo_root: Path, cell_id: str, output: Path) -> None:
    if cell_id not in required_cells(repo_root):
        raise EvidenceError(f"unknown packaged smoke cell {cell_id}")
    write_json(
        output,
        {
            "schema_version": CELL_SCHEMA_VERSION,
            "cell_id": cell_id,
            "source_commit": source_commit(repo_root),
            "status": "failed",
            "artifact_hashes": [],
            "operations": operation_entries("failed", 0),
            "failure": "packaged smoke cell did not reach a terminal pass",
        },
    )


def required_integer(value: object, field: str, minimum: int = 0) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < minimum:
        raise EvidenceError(f"{field} must be an integer greater than or equal to {minimum}")
    return value


def decode_app_result(raw: bytes) -> dict[str, object]:
    def reject_duplicate_members(pairs: list[tuple[str, object]]) -> dict[str, object]:
        value: dict[str, object] = {}
        for key, member in pairs:
            if key in value:
                raise EvidenceError("application phase result repeats a JSON member")
            value[key] = member
        return value

    try:
        value = json.loads(raw.decode("utf-8"), object_pairs_hook=reject_duplicate_members)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise EvidenceError("application phase result is malformed") from error
    return validate_app_result(value)


def validate_app_result(value: object, expected_phase: str | None = None) -> dict[str, object]:
    if not isinstance(value, dict):
        raise EvidenceError("application phase result must be an object")
    expected_keys = {
        "schema_version",
        "phase",
        "status",
        "pending_change_count",
        "observed",
        "error",
    }
    if set(value) != expected_keys:
        raise EvidenceError("application phase result has invalid members")
    schema_version = value.get("schema_version")
    phase = value.get("phase")
    if (
        not isinstance(schema_version, int)
        or isinstance(schema_version, bool)
        or schema_version != 1
        or not isinstance(phase, str)
        or phase not in {"initial", "resume"}
    ):
        raise EvidenceError("application phase result has invalid identity")
    if expected_phase is not None and phase != expected_phase:
        raise EvidenceError(f"application phase result is for {phase}, expected {expected_phase}")
    status = value.get("status")
    if not isinstance(status, str) or status not in {"passed", "failed"}:
        raise EvidenceError("application phase result has invalid status")
    pending_count = value.get("pending_change_count")
    if pending_count is not None:
        required_integer(pending_count, "application pending count")
    error = value.get("error")
    if status == "passed":
        if pending_count is None or error is not None:
            raise EvidenceError("passed application phase result is incomplete")
        validate_observed(value.get("observed"), f"{phase} application")
    elif (
        not isinstance(error, str)
        or not error
        or len(error) > APP_RESULT_MAX_ERROR_LENGTH
    ):
        raise EvidenceError("failed application phase result has invalid error detail")
    return value


def validate_observed(value: object, label: str) -> dict[str, str]:
    """Validate the rows that the consumer read through its public query path."""
    if (
        not isinstance(value, dict)
        or set(value) != set(OBSERVED_FIELDS)
        or not all(isinstance(item, str) for item in value.values())
    ):
        raise EvidenceError(f"{label} observed rows are invalid")
    return value


def json_value(text: str, label: str) -> object:
    try:
        return json.loads(text)
    except json.JSONDecodeError as error:
        raise EvidenceError(f"{label} is not JSON") from error


def app_result_path(results_dir: Path, phase: str) -> Path:
    if phase not in {"initial", "resume"}:
        raise EvidenceError("application result phase is invalid")
    return results_dir / f"{phase}.json"


class AppResultHTTPServer(http.server.HTTPServer):
    def __init__(self, address: tuple[str, int], results_dir: Path, token: str):
        super().__init__(address, AppResultRequestHandler)
        self.results_dir = results_dir
        self.token = token

    def server_bind(self) -> None:
        # A numeric loopback listener must not wait for reverse DNS.
        socketserver.TCPServer.server_bind(self)
        self.server_name, self.server_port = self.server_address[:2]


class AppResultRequestHandler(http.server.BaseHTTPRequestHandler):
    server: AppResultHTTPServer

    def setup(self) -> None:
        super().setup()
        self.connection.settimeout(APP_RESULT_READ_TIMEOUT_SECONDS)

    def log_message(self, format: str, *args: object) -> None:
        return

    def respond(self, status: int) -> None:
        body = b'{"status":"accepted"}' if status < 300 else b'{"status":"rejected"}'
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        self.respond(405)

    def do_POST(self) -> None:
        if self.path != APP_RESULT_PATH:
            self.respond(404)
            return
        authorization = self.headers.get("Authorization", "")
        prefix = "Bearer "
        supplied = authorization[len(prefix):] if authorization.startswith(prefix) else ""
        if not supplied or not hmac.compare_digest(supplied.encode("utf-8"), self.server.token.encode("ascii")):
            self.respond(401)
            return
        content_type = self.headers.get("Content-Type", "").split(";", 1)[0].strip().lower()
        if content_type != "application/json":
            self.respond(415)
            return
        raw_length = self.headers.get("Content-Length")
        try:
            content_length = int(raw_length) if raw_length is not None else 0
        except ValueError:
            content_length = 0
        if content_length <= 0 or content_length > APP_RESULT_MAX_BYTES:
            self.respond(413)
            return
        try:
            raw = self.rfile.read(content_length)
            if len(raw) != content_length:
                raise EvidenceError("application phase result body is incomplete")
            result = decode_app_result(raw)
        except (socket.timeout, TimeoutError):
            self.respond(408)
            return
        except EvidenceError:
            self.respond(400)
            return

        phase = str(result["phase"])
        target = app_result_path(self.server.results_dir, phase)
        if target.exists():
            try:
                existing = validate_app_result(load_json(target, f"{phase} application phase result"))
            except EvidenceError:
                self.respond(409)
                return
            self.respond(200 if existing == result else 409)
            return

        initial_path = app_result_path(self.server.results_dir, "initial")
        resume_path = app_result_path(self.server.results_dir, "resume")
        if resume_path.exists():
            self.respond(409)
            return
        if initial_path.exists():
            try:
                initial = validate_app_result(load_json(initial_path, "initial application phase result"), "initial")
            except EvidenceError:
                self.respond(409)
                return
            expected_phase = "resume" if initial["status"] == "passed" else None
        else:
            expected_phase = "initial"
        if phase != expected_phase:
            self.respond(409)
            return
        write_json(target, result, mode=0o600)
        self.respond(201)


def create_app_result_server(results_dir: Path) -> AppResultHTTPServer:
    try:
        results_dir.mkdir(parents=True, exist_ok=False)
    except FileExistsError as error:
        raise EvidenceError("application result directory must be fresh") from error
    collector_token = secrets.token_urlsafe(32)
    return AppResultHTTPServer(("127.0.0.1", 0), results_dir, collector_token)


def run_app_result_collector(ready_path: Path, results_dir: Path) -> None:
    server = create_app_result_server(results_dir)
    host, port = server.server_address
    write_json(
        ready_path,
        {
            "schema_version": 1,
            "url": f"http://{host}:{port}{APP_RESULT_PATH}",
            "token": server.token,
        },
        mode=0o600,
    )
    stopped = False

    def stop_collector(_signal: int, _frame: object) -> None:
        nonlocal stopped
        stopped = True

    signal.signal(signal.SIGINT, stop_collector)
    signal.signal(signal.SIGTERM, stop_collector)
    server.timeout = 0.5
    try:
        while not stopped:
            server.handle_request()
    finally:
        server.server_close()


def await_app_result(
    result_path: Path,
    phase: str,
    pid: int,
    output: Path,
    timeout_seconds: float,
) -> None:
    if not math.isfinite(timeout_seconds) or timeout_seconds <= 0 or timeout_seconds > 600:
        raise EvidenceError("application result timeout is outside the supported bound")
    deadline = time.monotonic() + timeout_seconds
    while not result_path.is_file():
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise EvidenceError(f"{phase} application phase result is missing")
        time.sleep(min(0.1, remaining))
    result = validate_app_result(load_json(result_path, f"{phase} application phase result"), phase)
    if result["status"] != "passed":
        raise EvidenceError(f"{phase} application reported failure: {result['error']}")
    pending_count = required_integer(result["pending_change_count"], f"{phase} application pending count")
    if phase == "initial" and pending_count != 1:
        raise EvidenceError("initial application did not report one durable pending change")
    if phase == "resume" and pending_count != 0:
        raise EvidenceError("resume application did not report a drained durable queue")
    write_json(
        output,
        {
            "schema_version": 1,
            "phase": phase,
            "status": "passed",
            "pid": required_integer(pid, f"{phase} pid", 1),
            "pending_change_count": pending_count,
            "observed": result["observed"],
        },
    )


def validate_phase(path: Path, expected_phase: str) -> dict[str, object]:
    value = load_json(path, f"{expected_phase} phase result")
    if not isinstance(value, dict):
        raise EvidenceError(f"{expected_phase} phase result must be an object")
    expected_keys = {"schema_version", "phase", "status", "pid", "pending_change_count", "observed"}
    if set(value) != expected_keys:
        raise EvidenceError(f"{expected_phase} phase result has invalid members")
    if value.get("schema_version") != 1 or value.get("phase") != expected_phase:
        raise EvidenceError(f"{expected_phase} phase result has invalid identity")
    if value.get("status") != "passed":
        raise EvidenceError(f"{expected_phase} phase did not pass")
    required_integer(value.get("pid"), f"{expected_phase} pid", 1)
    required_integer(value.get("pending_change_count"), f"{expected_phase} pending count")
    validate_observed(value.get("observed"), f"{expected_phase} phase")
    return value


def psql_rows(sql: str, variables: dict[str, str]) -> list[str]:
    database_url = os.environ.get("ADAPTER_TEST_URL", "").strip()
    if not database_url:
        raise EvidenceError("ADAPTER_TEST_URL is required for independent server checks")
    command = [os.environ.get("PACKAGED_SMOKE_PSQL", "").strip() or "psql", "--dbname", database_url, "-XAtq", "-v", "ON_ERROR_STOP=1"]
    for name, item in variables.items():
        command.extend(["-v", f"{name}={item}"])
    try:
        result = subprocess.run(command, input=sql, text=True, capture_output=True, timeout=60, check=False)
    except (OSError, subprocess.TimeoutExpired) as error:
        raise EvidenceError(f"independent server query did not run: {error}") from error
    if result.returncode != 0:
        raise EvidenceError("independent server query failed")
    return result.stdout.splitlines()


DATETIME_SQL = "'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"'"
# The same canonical text as dataset.SourceRowsSQL: trimmed decimals, UTC
# microsecond datetimes, and int64 as text so that no digit can round.
SERVER_STATE_SQL = f"""SELECT json_build_object(
  'program', (SELECT json_build_object(
      'organization_id', organization_id::text, 'owner_id', owner_id, 'title', title,
      'description', description, 'visibility', visibility, 'week_count', week_count,
      'settings', settings, 'deleted', deleted_at IS NOT NULL)
    FROM public.programs WHERE id = :'program_id'::uuid),
  'workout', (SELECT json_build_object(
      'program_id', program_id::text, 'owner_id', owner_id, 'name', name, 'notes', notes,
      'scheduled_on', to_char(scheduled_on, 'YYYY-MM-DD'),
      'started_at', to_char(started_at AT TIME ZONE 'UTC', {DATETIME_SQL}),
      'duration_seconds', duration_seconds, 'duration_minutes', duration_minutes,
      'total_volume_kg', trim_scale(total_volume_kg)::text, 'external_ref', external_ref::text,
      'deleted', deleted_at IS NOT NULL)
    FROM public.workouts WHERE id = :'workout_id'::uuid),
  'entry', (SELECT json_build_object(
      'workout_id', workout_id::text, 'program_id', program_id::text, 'exercise_id', exercise_id::text,
      'owner_id', owner_id, 'position', position, 'target', target, 'deleted', deleted_at IS NOT NULL)
    FROM public.workout_exercises WHERE id = :'entry_id'::uuid),
  'sets', (SELECT json_agg(json_build_object(
      'id', id::text, 'workout_id', workout_id::text, 'program_id', program_id::text,
      'owner_id', owner_id, 'set_index', set_index, 'reps', reps,
      'weight_kg', trim_scale(weight_kg)::text, 'rpe', rpe::text, 'duration_ms', duration_ms::text,
      'completed_at', to_char(completed_at AT TIME ZONE 'UTC', {DATETIME_SQL}), 'note', note,
      'deleted', deleted_at IS NOT NULL) ORDER BY set_index)
    FROM public.exercise_sets WHERE workout_exercise_id = :'entry_id'::uuid));
"""


def dataset_ids(config: dict[str, object]) -> dict[str, str]:
    return {name: str(config[name]) for name in ("program_id", "workout_id", "entry_id")}


def server_rows(config: dict[str, object]) -> dict[str, object]:
    rows = psql_rows(SERVER_STATE_SQL, dataset_ids(config))
    if len(rows) != 1:
        raise EvidenceError("server state query did not return one row")
    value = json_value(rows[0], "server row state")
    if not isinstance(value, dict):
        raise EvidenceError("server row state is invalid")
    return value


def consumer_sets(config: dict[str, object], durable: bool, remote: bool) -> list[tuple[str, tuple]]:
    """Return the authored (set_id, values) rows of one consumer state."""
    result = []
    for set_id, values in zip(config["set_ids"], CLIENT_SETS):
        if durable and values[0] == DURABLE_SET_INDEX:
            values = (values[0], DURABLE_REPS, *values[2:])
        result.append((str(set_id), values))
    if remote:
        result.extend((str(set_id), values) for set_id, values in zip(config["remote_set_ids"], REMOTE_SETS))
    return result


def expected_server_rows(config: dict[str, object], title: str, sets: list[tuple[str, tuple]], total: str) -> dict[str, object]:
    ids = dataset_ids(config)
    live = {"owner_id": str(config["user_id"]), "deleted": False}
    return {
        "program": {**AUTHORED_PROGRAM, "title": title, "organization_id": DATASET_ORGANIZATION_ID, **live},
        "workout": {**AUTHORED_WORKOUT, "program_id": ids["program_id"], "total_volume_kg": total, **live},
        # The server BEFORE triggers copy the parent chain onto each child row.
        "entry": {
            **AUTHORED_ENTRY, "workout_id": ids["workout_id"], "program_id": ids["program_id"],
            "exercise_id": DATASET_EXERCISE_ID, **live,
        },
        "sets": [
            {
                "id": set_id, "workout_id": ids["workout_id"], "program_id": ids["program_id"],
                "set_index": index, "reps": reps, "weight_kg": weight, "rpe": rpe,
                "duration_ms": duration, "completed_at": completed, "note": note, **live,
            }
            for set_id, (index, reps, weight, rpe, duration, completed, note) in sets
        ],
    }


def differing_fields(actual: object, expected: object, path: str = "row") -> list[str]:
    if isinstance(actual, dict) and isinstance(expected, dict) and set(actual) == set(expected):
        return [field for key in sorted(expected) for field in differing_fields(actual[key], expected[key], f"{path}.{key}")]
    if isinstance(actual, list) and isinstance(expected, list) and len(actual) == len(expected):
        return [field for index, pair in enumerate(zip(actual, expected)) for field in differing_fields(*pair, f"{path}[{index}]")]
    return [] if actual == expected else [path]


def require_server_rows(config: dict[str, object], expected: dict[str, object], message: str) -> None:
    actual = server_rows(config)
    if actual != expected:
        raise EvidenceError(f"{message}: {', '.join(differing_fields(actual, expected))}")


def sql_literal(value: object) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, dict):
        value = json.dumps(value, separators=(",", ":"), sort_keys=True)
    return "'" + str(value).replace("'", "''") + "'"


def set_values_sql(values: tuple) -> str:
    """Return set_index through note of one authored set as SQL literals."""
    index, reps, weight, rpe, duration, completed, note = values
    return ", ".join((str(index), str(reps), sql_literal(weight), rpe, duration or "NULL", sql_literal(completed), sql_literal(note)))


def client_statements(config: dict[str, object]) -> dict[str, object]:
    """Author the SQL that the installed consumer runs through its public API.

    The statements carry only the consumer inputs, never an expected server
    result. The parent-chain columns stay NULL, so the server triggers fill them.
    """
    user = sql_literal(config["user_id"])
    ids = {name: sql_literal(value) for name, value in dataset_ids(config).items()}
    stamp = sql_literal(AUTHORED_TIMESTAMP)
    program, workout, entry = AUTHORED_PROGRAM, AUTHORED_WORKOUT, AUTHORED_ENTRY
    sets = ", ".join(
        f"({sql_literal(set_id)}, {ids['entry_id']}, NULL, NULL, {user}, {set_values_sql(values)}, {stamp}, {stamp}, NULL)"
        for set_id, values in consumer_sets(config, durable=False, remote=False)
    )
    durable_id = sql_literal(config["set_ids"][DURABLE_SET_INDEX - 1])
    live_sets = f"FROM exercise_sets WHERE workout_exercise_id = {ids['entry_id']} AND deleted_at IS NULL"
    return {
        "initial_sql": [
            "INSERT INTO programs (id, organization_id, owner_id, title, description, visibility, week_count, settings, created_at, updated_at, deleted_at) "
            f"VALUES ({ids['program_id']}, {sql_literal(DATASET_ORGANIZATION_ID)}, {user}, {sql_literal(program['title'])}, "
            f"{sql_literal(program['description'])}, {sql_literal(program['visibility'])}, {program['week_count']}, "
            f"{sql_literal(program['settings'])}, {stamp}, {stamp}, NULL)",
            "INSERT INTO workouts (id, program_id, owner_id, name, notes, scheduled_on, started_at, duration_seconds, total_volume_kg, external_ref, created_at, updated_at, deleted_at) "
            f"VALUES ({ids['workout_id']}, {ids['program_id']}, {user}, {sql_literal(workout['name'])}, {sql_literal(workout['notes'])}, "
            f"{sql_literal(workout['scheduled_on'])}, {sql_literal(workout['started_at'])}, {workout['duration_seconds']}, '0', "
            f"{workout['external_ref']}, {stamp}, {stamp}, NULL)",
            "INSERT INTO workout_exercises (id, workout_id, program_id, exercise_id, owner_id, position, target, created_at, updated_at, deleted_at) "
            f"VALUES ({ids['entry_id']}, {ids['workout_id']}, NULL, {sql_literal(DATASET_EXERCISE_ID)}, {user}, "
            f"{entry['position']}, {sql_literal(entry['target'])}, {stamp}, {stamp}, NULL)",
            "INSERT INTO exercise_sets (id, workout_exercise_id, workout_id, program_id, owner_id, set_index, reps, weight_kg, rpe, duration_ms, completed_at, note, created_at, updated_at, deleted_at) "
            f"VALUES {sets}",
        ],
        "durable_sql": [
            f"UPDATE exercise_sets SET reps = {DURABLE_REPS}, updated_at = {sql_literal(DURABLE_TIMESTAMP)} WHERE id = {durable_id}",
        ],
        # converged is '1' when the pulled server total equals the sum of the
        # local live sets. It compares two local values and holds no expected one.
        "observe_sql": (
            f"SELECT (SELECT name FROM exercises WHERE id = {sql_literal(DATASET_EXERCISE_ID)}) AS exercise_name, "
            f"(SELECT title FROM programs WHERE id = {ids['program_id']}) AS program_title, "
            f"(SELECT total_volume_kg FROM workouts WHERE id = {ids['workout_id']}) AS total_volume_kg, "
            "(SELECT group_concat(set_index || ':' || reps || ':' || weight_kg, ',') FROM "
            f"(SELECT set_index, reps, weight_kg {live_sets} ORDER BY set_index)) AS sets, "
            f"CASE WHEN (SELECT CAST(total_volume_kg AS REAL) FROM workouts WHERE id = {ids['workout_id']}) = "
            f"(SELECT SUM(reps * CAST(weight_kg AS REAL)) {live_sets}) THEN '1' ELSE '0' END AS converged"
        ),
    }


def load_config(path: Path) -> dict[str, object]:
    value = load_json(path, "packaged smoke config")
    if not isinstance(value, dict) or value.get("schema_version") != 1:
        raise EvidenceError("packaged smoke config is invalid")
    return value


def author_remote_value(config_path: Path, output: Path) -> None:
    config = load_config(config_path)
    # The durable offline write must still be absent, or the resume cannot prove its own upload.
    require_server_rows(
        config,
        expected_server_rows(config, AUTHORED_PROGRAM["title"], consumer_sets(config, durable=False, remote=False), INITIAL_TOTAL_VOLUME_KG),
        "server does not hold exactly the initial upload before resume",
    )
    remote_title = f"Server authored {uuid.uuid4()} \u00e9\u4e16"
    owner = sql_literal(config["user_id"])
    entry = sql_literal(config["entry_id"])
    sets = ", ".join(
        f"({sql_literal(set_id)}, {entry}, {owner}, {set_values_sql(values)})"
        for set_id, values in zip(config["remote_set_ids"], REMOTE_SETS)
    )
    # One multi-row statement fires the rollup AFTER ROW trigger for each row (#178).
    rows = psql_rows(
        f"""BEGIN;
INSERT INTO public.exercise_sets (id, workout_exercise_id, owner_id, set_index, reps, weight_kg, rpe, duration_ms, completed_at, note)
VALUES {sets};
UPDATE public.programs SET title = :'remote_title', updated_at = clock_timestamp()
WHERE id = :'program_id'::uuid AND title = :'authored_title'
RETURNING title;
SELECT trim_scale(total_volume_kg)::text FROM public.workouts WHERE id = :'workout_id'::uuid;
COMMIT;
""",
        {**dataset_ids(config), "remote_title": remote_title, "authored_title": str(AUTHORED_PROGRAM["title"])},
    )
    if rows != [remote_title, REMOTE_TOTAL_VOLUME_KG]:
        raise EvidenceError("server did not author exactly the remote sets and program title")
    write_json(output, {"schema_version": 1, "remote_value": remote_title})


def load_remote_value(path: Path) -> str:
    value = load_json(path, "remote value record")
    if (
        not isinstance(value, dict)
        or set(value) != {"schema_version", "remote_value"}
        or value.get("schema_version") != 1
        or not isinstance(value.get("remote_value"), str)
        or not value["remote_value"].startswith("Server authored ")
    ):
        raise EvidenceError("remote value record is invalid")
    return value["remote_value"]


def verify_server_state(config_path: Path, remote_path: Path, output: Path) -> None:
    config = load_config(config_path)
    remote_title = load_remote_value(remote_path)
    require_server_rows(
        config,
        expected_server_rows(config, remote_title, consumer_sets(config, durable=True, remote=True), FINAL_TOTAL_VOLUME_KG),
        "server does not hold exactly the resumed upload and the remote value",
    )
    write_json(output, server_verification(remote_title, CLIENT_RESUMED_WRITE))


def server_verification(remote_value: str, resumed_write: dict[str, object]) -> dict[str, object]:
    return {"schema_version": 1, "status": "passed", "remote_value": remote_value, "resumed_write": resumed_write}


def validate_server_verification(path: Path, remote_value: str, resumed_write: dict[str, object]) -> None:
    if load_json(path, "server verification") != server_verification(remote_value, resumed_write):
        raise EvidenceError("server verification does not confirm the resumed upload and remote value")


def require_observed(phase: dict[str, object], label: str, expected: dict[str, str]) -> None:
    observed = validate_observed(phase["observed"], label)
    if observed != expected:
        raise EvidenceError(f"{label} consumer did not read the authored dataset state: {', '.join(differing_fields(observed, expected))}")


def complete_cell(
    repo_root: Path,
    cell_id: str,
    output: Path,
    initial_path: Path,
    resume_path: Path,
    killed_pid: int,
    artifacts: list[Path],
    expected_hashes: list[str],
    remote_path: Path,
    server_path: Path,
) -> None:
    if cell_id not in required_cells(repo_root):
        raise EvidenceError(f"unknown packaged smoke cell {cell_id}")
    initial = validate_phase(initial_path, "initial")
    resume = validate_phase(resume_path, "resume")
    remote_title = load_remote_value(remote_path)
    initial_observed = {
        "exercise_name": DATASET_EXERCISE_NAME,
        "program_title": str(AUTHORED_PROGRAM["title"]),
        "total_volume_kg": INITIAL_TOTAL_VOLUME_KG,
        "sets": INITIAL_OBSERVED_SETS,
    }
    require_observed(initial, "initial", initial_observed)
    require_observed(resume, "resume", {
        **initial_observed, "program_title": remote_title,
        "total_volume_kg": FINAL_TOTAL_VOLUME_KG, "sets": FINAL_OBSERVED_SETS,
    })
    validate_server_verification(server_path, remote_title, CLIENT_RESUMED_WRITE)
    initial_pid = required_integer(initial["pid"], "initial pid", 1)
    resume_pid = required_integer(resume["pid"], "resume pid", 1)
    if killed_pid != initial_pid:
        raise EvidenceError("killed pid does not match the initial consumer process")
    if resume_pid == initial_pid:
        raise EvidenceError("resume reused the killed consumer process")
    pending_before = required_integer(initial["pending_change_count"], "initial pending count")
    pending_after = required_integer(resume["pending_change_count"], "resume pending count")
    if pending_before <= 0:
        raise EvidenceError("initial consumer did not persist pending work before process kill")
    if pending_after != 0:
        raise EvidenceError("resumed consumer did not drain its durable pending work")
    hashes = hash_files(artifacts)
    if not hashes:
        raise EvidenceError("packaged smoke cell has no packaged artifact hash")
    expected = validate_hash_list(expected_hashes, "expected artifact hashes")
    if sorted(hashes) != sorted(expected):
        raise EvidenceError("packaged smoke artifact hashes do not match sealed artifact hashes")
    write_json(
        output,
        {
            "schema_version": CELL_SCHEMA_VERSION,
            "cell_id": cell_id,
            "source_commit": source_commit(repo_root),
            "status": "passed",
            "artifact_hashes": hashes,
            "operations": operation_entries("passed", 1),
            "process_lifecycle": {
                "initial_pid": initial_pid,
                "kill_signal": 9,
                "resume_pid": resume_pid,
                "durable_pending_before_kill": pending_before,
                "durable_pending_after_resume": pending_after,
                "remote_value_applied": True,
                "server_resumed_write_verified": True,
            },
        },
    )


def validate_server_phase(path: Path, expected_phase: str) -> dict[str, object]:
    value = load_json(path, f"server {expected_phase} phase result")
    expected = {"schema_version", "phase", "status", "adapter_pid", "push_digest"}
    if expected_phase == "resume":
        expected.update({"replay_equal", "observed_customer_name"})
    if not isinstance(value, dict) or set(value) != expected:
        raise EvidenceError(f"server {expected_phase} phase result has invalid members")
    if value.get("schema_version") != 1 or value.get("phase") != expected_phase or value.get("status") != "passed":
        raise EvidenceError(f"server {expected_phase} phase result is invalid")
    required_integer(value.get("adapter_pid"), f"server {expected_phase} adapter pid", 1)
    if not isinstance(value.get("push_digest"), str) or not SHA256.fullmatch(value["push_digest"]):
        raise EvidenceError(f"server {expected_phase} push digest is invalid")
    if expected_phase == "resume" and value.get("replay_equal") is not True:
        raise EvidenceError("server replay response differs from the persisted response")
    return value


def complete_server_cell(repo_root: Path, cell_id: str, output: Path, initial_path: Path, resume_path: Path, killed_pid: int, artifacts: list[Path], expected_hashes: list[str], remote_path: Path, server_path: Path) -> None:
    if cell_id not in required_cells(repo_root):
        raise EvidenceError(f"unknown packaged smoke cell {cell_id}")
    initial = validate_server_phase(initial_path, "initial")
    resume = validate_server_phase(resume_path, "resume")
    remote_name = load_remote_value(remote_path)
    if resume["observed_customer_name"] != remote_name:
        raise EvidenceError("resumed server consumer did not pull the remote value")
    validate_server_verification(server_path, remote_name, SERVER_OFFLINE_WRITE)
    initial_pid = required_integer(initial["adapter_pid"], "server initial adapter pid", 1)
    resume_pid = required_integer(resume["adapter_pid"], "server resume adapter pid", 1)
    if killed_pid != initial_pid or resume_pid == initial_pid:
        raise EvidenceError("server adapter process replacement proof is invalid")
    if initial["push_digest"] != resume["push_digest"]:
        raise EvidenceError("server replay digest differs from the persisted push")
    hashes = hash_files(artifacts)
    if sorted(hashes) != sorted(validate_hash_list(expected_hashes, "expected artifact hashes")):
        raise EvidenceError("packaged smoke artifact hashes do not match sealed artifact hashes")
    write_json(
        output,
        {
            "schema_version": CELL_SCHEMA_VERSION,
            "cell_id": cell_id,
            "source_commit": source_commit(repo_root),
            "status": "passed",
            "artifact_hashes": hashes,
            "operations": operation_entries("passed", 1),
            "process_lifecycle": {
                "kind": "server",
                "initial_pid": initial_pid,
                "kill_signal": 9,
                "resume_pid": resume_pid,
                "push_digest": initial["push_digest"],
                "replay_equal": True,
                "remote_value_applied": True,
                "server_resumed_write_verified": True,
            },
        },
    )


def validate_hash_list(value: object, field: str, allow_empty: bool = False) -> list[str]:
    if not isinstance(value, list) or (not value and not allow_empty):
        raise EvidenceError(f"{field} must contain artifact hashes")
    result: list[str] = []
    for item in value:
        if not isinstance(item, str) or not SHA256.fullmatch(item):
            raise EvidenceError(f"{field} contains an invalid artifact hash")
        result.append(item)
    if len(result) != len(set(result)):
        raise EvidenceError(f"{field} repeats an artifact hash")
    return result


def validate_cell(value: object, expected_cell: str, expected_commit: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise EvidenceError(f"cell {expected_cell} evidence must be an object")
    if value.get("schema_version") != CELL_SCHEMA_VERSION:
        raise EvidenceError(f"cell {expected_cell} has the wrong schema version")
    if value.get("cell_id") != expected_cell:
        raise EvidenceError(f"cell {expected_cell} has the wrong identity")
    if value.get("source_commit") != expected_commit:
        raise EvidenceError(f"cell {expected_cell} source commit does not match HEAD")
    status = value.get("status")
    if status not in {"passed", "failed"}:
        raise EvidenceError(f"cell {expected_cell} has an invalid status")
    expected_members = {
        "schema_version",
        "cell_id",
        "source_commit",
        "status",
        "artifact_hashes",
        "operations",
        "process_lifecycle" if status == "passed" else "failure",
    }
    if set(value) != expected_members:
        raise EvidenceError(f"cell {expected_cell} has invalid members")
    failure = value.get("failure")
    if status == "failed" and (not isinstance(failure, str) or not failure):
        raise EvidenceError(f"cell {expected_cell} failure is missing")
    hashes = validate_hash_list(
        value.get("artifact_hashes"),
        f"cell {expected_cell} artifact_hashes",
        allow_empty=status == "failed",
    )
    operations = value.get("operations")
    if not isinstance(operations, list):
        raise EvidenceError(f"cell {expected_cell} operations are missing")
    actual_names: list[str] = []
    for item in operations:
        if not isinstance(item, dict):
            raise EvidenceError(f"cell {expected_cell} contains a malformed operation")
        if set(item) != {"name", "status", "terminal", "test_count"}:
            raise EvidenceError(f"cell {expected_cell} contains invalid operation members")
        name = item.get("name")
        if not isinstance(name, str):
            raise EvidenceError(f"cell {expected_cell} contains an unnamed operation")
        actual_names.append(name)
        if item.get("status") != status:
            raise EvidenceError(f"cell {expected_cell} operation {name} has the wrong status")
        if item.get("terminal") is not True:
            raise EvidenceError(f"cell {expected_cell} operation {name} is not terminal")
        expected_count = 1 if status == "passed" else 0
        if item.get("test_count") != expected_count:
            raise EvidenceError(f"cell {expected_cell} operation {name} has an invalid test count")
    if actual_names != list(SMOKE_OPERATIONS):
        raise EvidenceError(f"cell {expected_cell} does not contain each smoke operation once")
    if status == "passed":
        lifecycle = value.get("process_lifecycle")
        if not isinstance(lifecycle, dict):
            raise EvidenceError(f"cell {expected_cell} process lifecycle is missing")
        kind = lifecycle.get("kind", "client")
        common_keys = {"initial_pid", "kill_signal", "resume_pid"}
        convergence_keys = {"remote_value_applied", "server_resumed_write_verified"}
        client_keys = common_keys | convergence_keys | {"durable_pending_before_kill", "durable_pending_after_resume"}
        server_keys = common_keys | convergence_keys | {"kind", "push_digest", "replay_equal"}
        lifecycle_keys = set(lifecycle)
        if lifecycle_keys != client_keys and lifecycle_keys != server_keys:
            raise EvidenceError(f"cell {expected_cell} process lifecycle has invalid members")
        initial_pid = required_integer(lifecycle.get("initial_pid"), "initial pid", 1)
        resume_pid = required_integer(lifecycle.get("resume_pid"), "resume pid", 1)
        if lifecycle.get("kill_signal") != 9 or initial_pid == resume_pid:
            raise EvidenceError(f"cell {expected_cell} process kill proof is invalid")
        if lifecycle.get("remote_value_applied") is not True or lifecycle.get("server_resumed_write_verified") is not True:
            raise EvidenceError(f"cell {expected_cell} did not prove bidirectional convergence")
        if kind == "server":
            if (
                not isinstance(lifecycle.get("push_digest"), str)
                or not SHA256.fullmatch(lifecycle["push_digest"])
                or lifecycle.get("replay_equal") is not True
            ):
                raise EvidenceError(f"cell {expected_cell} server replay proof is invalid")
        else:
            if required_integer(lifecycle.get("durable_pending_before_kill"), "pending before kill") <= 0:
                raise EvidenceError(f"cell {expected_cell} has no durable work before kill")
            if lifecycle.get("durable_pending_after_resume") != 0:
                raise EvidenceError(f"cell {expected_cell} did not drain durable work after resume")
    return {
        "status": status,
        "artifact_hashes": hashes,
        "operations": operations,
    }


def missing_cell(cell_id: str) -> dict[str, object]:
    return {
        "status": "failed",
        "artifact_hashes": [],
        "operations": operation_entries("failed", 0),
        "failure": f"cell evidence is missing for {cell_id}",
    }


def collect_summary(repo_root: Path, cells_dir: Path, output: Path) -> None:
    commit = source_commit(repo_root)
    cells = required_cells(repo_root)
    if cells_dir.exists():
        evidence_cells = {path.stem for path in cells_dir.glob("*.json")}
        unexpected = evidence_cells.difference(cells)
        if unexpected:
            raise EvidenceError(f"packaged smoke has unexpected cell evidence: {', '.join(sorted(unexpected))}")
    records: dict[str, dict[str, object]] = {}
    for cell_id in cells:
        path = cells_dir / f"{cell_id}.json"
        if not path.is_file():
            records[cell_id] = missing_cell(cell_id)
            continue
        records[cell_id] = validate_cell(load_json(path, f"cell {cell_id}"), cell_id, commit)

    hashes = sorted(
        {
            digest
            for record in records.values()
            for digest in record.get("artifact_hashes", [])
            if isinstance(digest, str)
        }
    )
    obligations: list[dict[str, object]] = []
    all_passed = True
    for cell_id in cells:
        record = records[cell_id]
        record_status = record["status"]
        if record_status != "passed":
            all_passed = False
        record_hashes = record.get("artifact_hashes", [])
        for operation in record["operations"]:
            if not isinstance(operation, dict):
                raise EvidenceError(f"cell {cell_id} contains a malformed operation")
            obligations.append(
                {
                    "id": f"smoke/{cell_id}/{operation['name']}",
                    "kind": "smoke",
                    "status": operation["status"],
                    "terminal": operation["terminal"],
                    "test_count": operation["test_count"],
                    "artifact_hashes": record_hashes,
                }
            )
    write_json(
        output,
        {
            "schema_version": SUMMARY_SCHEMA_VERSION,
            "source_commit": commit,
            "artifact_hashes": hashes,
            "obligations": obligations,
            "status": "passed" if all_passed else "failed",
        },
    )


def verify_summary(repo_root: Path, summary_path: Path, release_manifest: Path | None = None) -> None:
    value = load_json(summary_path, "packaged smoke summary")
    required_keys = {
        "schema_version",
        "source_commit",
        "artifact_hashes",
        "obligations",
        "status",
    }
    if not isinstance(value, dict) or set(value) != required_keys:
        raise EvidenceError("packaged smoke summary has invalid members")
    if value.get("schema_version") != SUMMARY_SCHEMA_VERSION:
        raise EvidenceError("packaged smoke summary has the wrong schema version")
    if value.get("source_commit") != source_commit(repo_root):
        raise EvidenceError("packaged smoke summary source commit does not match HEAD")
    if value.get("status") != "passed":
        raise EvidenceError("packaged smoke summary did not pass")
    summary_hashes = set(validate_hash_list(value.get("artifact_hashes"), "summary artifact hashes"))
    obligations = value.get("obligations")
    if not isinstance(obligations, list):
        raise EvidenceError("packaged smoke summary obligations are missing")
    expected_ids = {
        f"smoke/{cell_id}/{operation}"
        for cell_id in required_cells(repo_root)
        for operation in SMOKE_OPERATIONS
    }
    seen: set[str] = set()
    for obligation in obligations:
        if not isinstance(obligation, dict) or set(obligation) != {
            "id",
            "kind",
            "status",
            "terminal",
            "test_count",
            "artifact_hashes",
        }:
            raise EvidenceError("packaged smoke summary contains a malformed obligation")
        obligation_id = obligation.get("id")
        if obligation_id not in expected_ids or obligation_id in seen:
            raise EvidenceError("packaged smoke summary contains an unknown or duplicate obligation")
        seen.add(obligation_id)
        hashes = set(validate_hash_list(obligation.get("artifact_hashes"), f"{obligation_id} artifact hashes"))
        if (
            obligation.get("kind") != "smoke"
            or obligation.get("status") != "passed"
            or obligation.get("terminal") is not True
            or required_integer(obligation.get("test_count"), f"{obligation_id} test count", 1) < 1
            or not hashes.issubset(summary_hashes)
        ):
            raise EvidenceError(f"packaged smoke obligation did not pass: {obligation_id}")
    if seen != expected_ids:
        raise EvidenceError("packaged smoke summary has missing obligations")
    if release_manifest is not None:
        manifest = load_json(release_manifest, "sealed release manifest")
        source = manifest.get("source") if isinstance(manifest, dict) else None
        if not isinstance(source, dict) or source.get("commit") != value["source_commit"]:
            raise EvidenceError("packaged smoke and sealed source commits differ")
        distributions = manifest.get("distributions")
        if not isinstance(distributions, list):
            raise EvidenceError("sealed release distributions are missing")
        role_hashes = {}
        for record in distributions:
            if not isinstance(record, dict) or record.get("kind") != "file":
                continue
            role, digest = record.get("role"), record.get("sha256")
            if not isinstance(role, str) or role in role_hashes or not isinstance(digest, str) or not SHA256.fullmatch(digest):
                raise EvidenceError("sealed release contains invalid file identities")
            role_hashes[role] = digest
        manifest_hash = hash_files([release_manifest])[0]
        required_roles = {
            ("postgresql-server", "postgresql"): ("pg-extension", "adapter", "seed-tool"),
            ("swift-client", "ios"): (),
            ("kotlin-client", "android"): ("kotlin-maven",),
            ("react-native-client", "ios"): ("react-native-npm",),
            ("react-native-client", "android"): ("react-native-npm", "kotlin-maven"),
        }
        matrix = load_json(repo_root / "conformance/support-matrix.json", "support matrix")
        expected_hashes = {}
        for cell in matrix["cells"]:
            if cell["policy"] != "required":
                continue
            key = (cell["component"], cell["platform"])
            if key not in required_roles or any(role not in role_hashes for role in required_roles[key]):
                raise EvidenceError(f"sealed release does not cover cell {cell['id']}")
            hashes = {role_hashes[role] for role in required_roles[key]}
            if cell["platform"] == "ios":
                hashes.add(manifest_hash)
            expected_hashes[cell["id"]] = hashes
        for obligation in obligations:
            cell_id = obligation["id"].split("/")[1]
            if set(obligation["artifact_hashes"]) != expected_hashes[cell_id]:
                raise EvidenceError(f"packaged smoke cell {cell_id} does not match sealed payloads")
        if summary_hashes != set().union(*expected_hashes.values()):
            raise EvidenceError("packaged smoke summary hashes do not match sealed payloads")


def dry_summary(repo_root: Path, output: Path) -> None:
    commit = source_commit(repo_root)
    structural_hash = hashlib.sha256((repo_root / "verification/packaged-smoke-cell.schema.json").read_bytes()).hexdigest()
    obligations = [
        {
            "id": f"smoke/{cell_id}/{operation}",
            "kind": "smoke",
            "status": "passed",
            "terminal": True,
            "test_count": 1,
            "artifact_hashes": [structural_hash],
        }
        for cell_id in required_cells(repo_root)
        for operation in SMOKE_OPERATIONS
    ]
    write_json(
        output,
        {
            "schema_version": SUMMARY_SCHEMA_VERSION,
            "source_commit": commit,
            "artifact_hashes": [structural_hash],
            "obligations": obligations,
            "status": "passed",
            "dry_run": True,
        },
    )


def base64url(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def bearer_token(subject: str) -> str:
    supplied = os.environ.get("SYNCHRO_PACKAGED_SMOKE_TOKEN", "").strip()
    if supplied:
        return supplied
    secret = os.environ.get("SYNCHRO_TEST_JWT_SECRET", "")
    if not secret:
        secret_file = os.environ.get("SYNCHRO_CONFORMANCE_JWT_SECRET_FILE", "").strip()
        if secret_file:
            try:
                secret = Path(secret_file).read_text(encoding="utf-8").strip()
            except OSError as error:
                raise EvidenceError(f"cannot read the packaged smoke JWT secret file: {error}") from error
    if not secret:
        raise EvidenceError(
            "SYNCHRO_PACKAGED_SMOKE_TOKEN, SYNCHRO_TEST_JWT_SECRET, or "
            "SYNCHRO_CONFORMANCE_JWT_SECRET_FILE is required"
        )
    now = int(time.time())
    header = base64url(b'{"alg":"HS256","typ":"JWT"}')
    # Native compilation precedes execution within a six-hour hosted job.
    expires_at = now + 6 * 60 * 60
    payload = base64url(
        json.dumps(
            {"sub": subject, "iat": now, "exp": expires_at},
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    )
    signing_input = f"{header}.{payload}"
    signature = hmac.new(secret.encode("utf-8"), signing_input.encode("ascii"), hashlib.sha256).digest()
    return f"{signing_input}.{base64url(signature)}"


def smoke_config(cell_id: str, platform: str) -> dict[str, object]:
    server_url = os.environ.get("SYNCHRO_TEST_URL", "").strip().rstrip("/")
    parsed = urlsplit(server_url)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname or parsed.username or parsed.password:
        raise EvidenceError("SYNCHRO_TEST_URL must be an HTTP URL without embedded credentials")
    if parsed.query or parsed.fragment:
        raise EvidenceError("SYNCHRO_TEST_URL must not contain a query or fragment")
    user_id = str(uuid.uuid4())
    config: dict[str, object] = {
        "schema_version": 1,
        "cell_id": cell_id,
        "platform": platform,
        "server_url": server_url,
        "token": bearer_token(user_id),
        "user_id": user_id,
        "client_id": str(uuid.uuid4()),
        "program_id": str(uuid.uuid4()),
        "workout_id": str(uuid.uuid4()),
        "entry_id": str(uuid.uuid4()),
        "set_ids": [str(uuid.uuid4()) for _ in CLIENT_SETS],
        "remote_set_ids": [str(uuid.uuid4()) for _ in REMOTE_SETS],
        "phase": "initial",
    }
    config.update(client_statements(config))
    return config


def write_config(
    cell_id: str,
    platform: str,
    output: Path,
    collector_ready: Path | None = None,
) -> None:
    result = smoke_config(cell_id, platform)
    if collector_ready is not None:
        try:
            mode = collector_ready.stat().st_mode & 0o777
        except OSError as error:
            raise EvidenceError(f"application result collector configuration is missing: {error}") from error
        if mode != 0o600:
            raise EvidenceError("application result collector configuration must have mode 0600")
        collector = load_json(collector_ready, "application result collector configuration")
        if (
            not isinstance(collector, dict)
            or set(collector) != {"schema_version", "url", "token"}
            or collector.get("schema_version") != 1
            or isinstance(collector.get("schema_version"), bool)
        ):
            raise EvidenceError("application result collector configuration is invalid")
        result_url = collector.get("url")
        result_token = collector.get("token")
        if not isinstance(result_url, str) or not isinstance(result_token, str):
            raise EvidenceError("application result collector configuration is invalid")
        parsed_result = urlsplit(result_url)
        try:
            result_port = parsed_result.port
        except ValueError as error:
            raise EvidenceError("application result collector URL has an invalid port") from error
        if (
            parsed_result.scheme != "http"
            or parsed_result.hostname not in {"127.0.0.1", "localhost"}
            or result_port is None
            or parsed_result.path != APP_RESULT_PATH
            or parsed_result.query
            or parsed_result.fragment
            or parsed_result.username
            or parsed_result.password
            or len(result_token) < 32
            or len(result_token) > 256
        ):
            raise EvidenceError("application result collector configuration is invalid")
        result["result_url"] = result_url
        result["result_token"] = result_token
    write_json(output, result, mode=0o600)


def write_typescript(value: dict[str, object], output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    serialized = json.dumps(value, separators=(",", ":"), sort_keys=True)
    content = "export const packagedSmokeConfig = " + serialized + " as const;\n"
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{output.name}.", dir=output.parent)
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, output)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise


def config_to_typescript(config_path: Path, output: Path) -> None:
    value = load_json(config_path, "packaged smoke config")
    if not isinstance(value, dict) or value.get("schema_version") != 1:
        raise EvidenceError("packaged smoke config is invalid")
    write_typescript(value, output)


def config_value(path: Path, field: str) -> None:
    value = load_json(path, "packaged smoke config")
    if not isinstance(value, dict) or field not in value:
        raise EvidenceError(f"packaged smoke config field {field} is missing")
    item = value[field]
    if not isinstance(item, (str, int)) or isinstance(item, bool):
        raise EvidenceError(f"packaged smoke config field {field} is invalid")
    print(item)


def set_config_phase(path: Path, phase: str, output: Path) -> None:
    value = load_json(path, "packaged smoke config")
    if not isinstance(value, dict) or value.get("schema_version") != 1:
        raise EvidenceError("packaged smoke config is invalid")
    if phase not in {"initial", "resume"}:
        raise EvidenceError("packaged smoke phase is invalid")
    value["phase"] = phase
    write_json(output, value, mode=0o600)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    begin = subparsers.add_parser("begin-cell")
    begin.add_argument("--repo-root", type=Path, required=True)
    begin.add_argument("--cell", required=True)
    begin.add_argument("--output", type=Path, required=True)

    complete = subparsers.add_parser("complete-cell")
    complete.add_argument("--repo-root", type=Path, required=True)
    complete.add_argument("--cell", required=True)
    complete.add_argument("--output", type=Path, required=True)
    complete.add_argument("--initial", type=Path, required=True)
    complete.add_argument("--resume", type=Path, required=True)
    complete.add_argument("--killed-pid", type=int, required=True)
    complete.add_argument("--artifact", action="append", type=Path, default=[])
    complete.add_argument("--expected-artifact-hash", action="append", default=[])
    complete.add_argument("--remote", type=Path, required=True)
    complete.add_argument("--server-verification", type=Path, required=True)

    author = subparsers.add_parser("author-remote")
    author.add_argument("--config", type=Path, required=True)
    author.add_argument("--output", type=Path, required=True)

    verify_server = subparsers.add_parser("verify-server")
    verify_server.add_argument("--config", type=Path, required=True)
    verify_server.add_argument("--remote", type=Path, required=True)
    verify_server.add_argument("--output", type=Path, required=True)

    server_complete = subparsers.add_parser("complete-server-cell")
    for argument in ("--repo-root", "--cell", "--output", "--initial", "--resume"):
        server_complete.add_argument(argument, type=Path if argument in {"--repo-root", "--output", "--initial", "--resume"} else str, required=True)
    server_complete.add_argument("--killed-pid", type=int, required=True)
    server_complete.add_argument("--artifact", action="append", type=Path, default=[])
    server_complete.add_argument("--expected-artifact-hash", action="append", default=[])
    server_complete.add_argument("--remote", type=Path, required=True)
    server_complete.add_argument("--server-verification", type=Path, required=True)

    collect = subparsers.add_parser("collect")
    collect.add_argument("--repo-root", type=Path, required=True)
    collect.add_argument("--cells-dir", type=Path, required=True)
    collect.add_argument("--output", type=Path, required=True)

    verify = subparsers.add_parser("verify-summary")
    verify.add_argument("--repo-root", type=Path, required=True)
    verify.add_argument("--summary", type=Path, required=True)
    verify.add_argument("--release-manifest", type=Path)

    dry = subparsers.add_parser("dry-run")
    dry.add_argument("--repo-root", type=Path, required=True)
    dry.add_argument("--output", type=Path, required=True)

    config = subparsers.add_parser("config")
    config.add_argument("--cell", required=True)
    config.add_argument("--platform", required=True)
    config.add_argument("--collector-ready", type=Path)
    config.add_argument("--output", type=Path, required=True)

    config_ts = subparsers.add_parser("config-to-typescript")
    config_ts.add_argument("--config", type=Path, required=True)
    config_ts.add_argument("--output", type=Path, required=True)

    get = subparsers.add_parser("config-value")
    get.add_argument("--config", type=Path, required=True)
    get.add_argument("--field", required=True)

    set_phase = subparsers.add_parser("set-config-phase")
    set_phase.add_argument("--config", type=Path, required=True)
    set_phase.add_argument("--phase", choices=("initial", "resume"), required=True)
    set_phase.add_argument("--output", type=Path, required=True)

    collector = subparsers.add_parser("result-collector")
    collector.add_argument("--ready", type=Path, required=True)
    collector.add_argument("--results-dir", type=Path, required=True)

    await_result = subparsers.add_parser("await-app-result")
    await_result.add_argument("--result", type=Path, required=True)
    await_result.add_argument("--phase", choices=("initial", "resume"), required=True)
    await_result.add_argument("--pid", type=int, required=True)
    await_result.add_argument("--output", type=Path, required=True)
    await_result.add_argument("--timeout-seconds", type=float, required=True)

    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        if args.command == "begin-cell":
            begin_cell(args.repo_root.resolve(), args.cell, args.output.resolve())
        elif args.command == "complete-cell":
            complete_cell(
                args.repo_root.resolve(),
                args.cell,
                args.output.resolve(),
                args.initial.resolve(),
                args.resume.resolve(),
                args.killed_pid,
                [path.resolve() for path in args.artifact],
                args.expected_artifact_hash,
                args.remote.resolve(),
                args.server_verification.resolve(),
            )
        elif args.command == "author-remote":
            author_remote_value(args.config.resolve(), args.output.resolve())
        elif args.command == "verify-server":
            verify_server_state(args.config.resolve(), args.remote.resolve(), args.output.resolve())
        elif args.command == "complete-server-cell":
            complete_server_cell(args.repo_root.resolve(), args.cell, args.output.resolve(), args.initial.resolve(), args.resume.resolve(), args.killed_pid, [path.resolve() for path in args.artifact], args.expected_artifact_hash, args.remote.resolve(), args.server_verification.resolve())
        elif args.command == "collect":
            collect_summary(args.repo_root.resolve(), args.cells_dir.resolve(), args.output.resolve())
        elif args.command == "verify-summary":
            verify_summary(args.repo_root.resolve(), args.summary.resolve(), args.release_manifest)
        elif args.command == "dry-run":
            dry_summary(args.repo_root.resolve(), args.output.resolve())
        elif args.command == "config":
            write_config(
                args.cell,
                args.platform,
                args.output.resolve(),
                args.collector_ready.resolve() if args.collector_ready is not None else None,
            )
        elif args.command == "config-to-typescript":
            config_to_typescript(args.config.resolve(), args.output.resolve())
        elif args.command == "config-value":
            config_value(args.config.resolve(), args.field)
        elif args.command == "set-config-phase":
            set_config_phase(args.config.resolve(), args.phase, args.output.resolve())
        elif args.command == "result-collector":
            run_app_result_collector(args.ready.resolve(), args.results_dir.resolve())
        elif args.command == "await-app-result":
            await_app_result(
                args.result.resolve(),
                args.phase,
                args.pid,
                args.output.resolve(),
                args.timeout_seconds,
            )
        else:
            raise EvidenceError(f"unsupported command {args.command}")
    except EvidenceError as error:
        print(f"packaged-smoke: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
