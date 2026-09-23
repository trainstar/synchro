from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import math
import os
from pathlib import Path
import secrets
import signal
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request


PROTOCOL = "synchro-adapter-owner-v1"
MESSAGE_LIMIT = 4096


def state_path() -> Path:
    value = os.environ.get("SYNCHROD_ADAPTER_PID_FILE")
    if not value:
        raise RuntimeError("SYNCHROD_ADAPTER_PID_FILE is required")
    return Path(value).resolve()


def configuration() -> tuple[Path, str, str, float, str]:
    names = (
        "DATABASE_URL", "JWT_SECRET", "LISTEN_ADDR",
        "SYNCHROD_ADAPTER_BINARY", "SYNCHROD_ADAPTER_LOG_FILE",
    )
    if any(not os.environ.get(name) for name in names):
        raise RuntimeError("adapter environment is incomplete")
    binary = Path(os.environ["SYNCHROD_ADAPTER_BINARY"]).resolve()
    if not binary.is_file() or not os.access(binary, os.X_OK):
        raise RuntimeError("adapter binary is not executable")
    listen = os.environ["LISTEN_ADDR"]
    authority = "127.0.0.1" + listen if listen.startswith(":") else listen
    ready_url = os.environ.get("SYNCHROD_ADAPTER_READY_URL", f"http://{authority}/sync/schema")
    parsed = urllib.parse.urlsplit(ready_url)
    if parsed.scheme != "http" or parsed.username or parsed.password or not parsed.hostname or not parsed.port:
        raise RuntimeError("adapter readiness URL is invalid")
    timeout = float(os.environ.get("SYNCHROD_ADAPTER_READY_ATTEMPTS", "30"))
    if not math.isfinite(timeout) or timeout <= 0:
        raise RuntimeError("adapter readiness timeout must be finite and positive")
    identity = {name: os.environ[name] for name in names if name != "SYNCHROD_ADAPTER_LOG_FILE"}
    identity["MIN_CLIENT_VERSION"] = os.environ.get("MIN_CLIENT_VERSION", "")
    with binary.open("rb") as stream:
        identity["binary_sha256"] = hashlib.file_digest(stream, "sha256").hexdigest()
    fingerprint = hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()
    return binary, ready_url, fingerprint, timeout, str(parsed.port)


def read_state(path: Path) -> dict | None:
    try:
        data = path.read_bytes()
    except FileNotFoundError:
        return None
    if len(data) > MESSAGE_LIMIT:
        raise RuntimeError("adapter ownership record is too large")
    try:
        state = json.loads(data)
    except (ValueError, UnicodeDecodeError) as error:
        raise RuntimeError("adapter ownership record is invalid; no process was signaled") from error
    if (
        not isinstance(state, dict)
        or set(state) != {"protocol", "run_id", "port", "fingerprint"}
        or state["protocol"] != PROTOCOL
        or not isinstance(state["run_id"], str)
        or len(state["run_id"]) != 64
        or type(state["port"]) is not int
        or not 0 < state["port"] < 65536
    ):
        raise RuntimeError("adapter ownership record is unsupported; no process was signaled")
    return state


def request(state: dict, operation: str) -> dict:
    try:
        with socket.create_connection(("127.0.0.1", state["port"]), timeout=10) as connection:
            connection.sendall(json.dumps({
                "protocol": PROTOCOL, "run_id": state["run_id"], "operation": operation,
            }).encode() + b"\n")
            with connection.makefile("rb") as stream:
                data = stream.readline(MESSAGE_LIMIT + 1)
    except OSError as error:
        raise RuntimeError("adapter owner is unavailable; no unverified process was signaled") from error
    if len(data) > MESSAGE_LIMIT:
        raise RuntimeError("adapter owner response is too large")
    try:
        response = json.loads(data)
    except (ValueError, UnicodeDecodeError) as error:
        raise RuntimeError("adapter owner response is invalid") from error
    if not isinstance(response, dict) or not hmac.compare_digest(str(response.get("run_id", "")), state["run_id"]):
        raise RuntimeError("adapter owner identity does not match")
    if response.get("error"):
        raise RuntimeError("adapter owner rejected the request")
    return response


def listeners(port: str) -> set[int]:
    result = subprocess.run(
        ["lsof", "-nP", "-t", f"-iTCP:{port}", "-sTCP:LISTEN"],
        capture_output=True, text=True, timeout=5,
    )
    if result.returncode not in (0, 1) or result.stderr:
        raise RuntimeError("cannot inspect adapter listener ownership")
    return {int(value) for value in result.stdout.split()}


def stop_child(child: subprocess.Popen, timeout: float = 5) -> None:
    if child.poll() is not None:
        return
    child.terminate()
    try:
        child.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        child.kill()
        child.wait(timeout=5)


def serve() -> int:
    binary, ready_url, fingerprint, timeout, port = configuration()
    path = state_path()
    if listeners(port):
        raise RuntimeError("adapter port is occupied; no process was signaled")
    path.parent.mkdir(parents=True, exist_ok=True)
    run_id = secrets.token_hex(32)
    stopping = False

    def stop_requested(_signum, _frame):
        nonlocal stopping
        stopping = True

    for signum in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
        signal.signal(signum, stop_requested)
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    with socket.socket() as control:
        control.bind(("127.0.0.1", 0))
        control.listen()
        control.settimeout(0.1)
        state = {
            "protocol": PROTOCOL, "run_id": run_id,
            "port": control.getsockname()[1], "fingerprint": fingerprint,
        }
        temporary = None
        child = None
        published = False
        try:
            with tempfile.NamedTemporaryFile(mode="w", dir=path.parent, delete=False) as stream:
                temporary = Path(stream.name)
                json.dump(state, stream)
                stream.flush()
                os.fsync(stream.fileno())
            os.link(temporary, path)
            published = True
            temporary.unlink()
            temporary = None
            child = subprocess.Popen([str(binary)], stdin=subprocess.DEVNULL)
            deadline = time.monotonic() + timeout
            ready = False
            while not stopping:
                if child.poll() is not None:
                    raise RuntimeError("adapter exited before shutdown; inspect its log file")
                if not ready:
                    owners = listeners(port)
                    if owners - {child.pid}:
                        raise RuntimeError("another process owns the adapter port")
                    if owners == {child.pid}:
                        try:
                            with opener.open(ready_url, timeout=0.5) as response:
                                ready = response.status == 200
                        except (urllib.error.URLError, TimeoutError):
                            ready = False
                    if not ready and time.monotonic() >= deadline:
                        raise RuntimeError("adapter readiness deadline expired")
                try:
                    connection, _ = control.accept()
                except socket.timeout:
                    continue
                with connection:
                    connection.settimeout(2)
                    try:
                        with connection.makefile("rb") as stream:
                            data = stream.readline(MESSAGE_LIMIT + 1)
                        incoming = json.loads(data) if len(data) <= MESSAGE_LIMIT else None
                    except (OSError, ValueError, UnicodeDecodeError):
                        continue
                    if (
                        not isinstance(incoming, dict)
                        or set(incoming) != {"protocol", "run_id", "operation"}
                        or incoming.get("protocol") != PROTOCOL
                        or not isinstance(incoming.get("run_id"), str)
                        or not hmac.compare_digest(incoming["run_id"], run_id)
                    ):
                        continue
                    operation = incoming.get("operation")
                    response = {"run_id": run_id, "ready": ready, "pid": child.pid}
                    if operation == "stop":
                        stop_child(child)
                        if read_state(path) == state:
                            path.unlink()
                            published = False
                        response["stopped"] = True
                        stopping = True
                    elif operation != "status":
                        response["error"] = True
                    try:
                        connection.sendall(json.dumps(response).encode() + b"\n")
                    except OSError:
                        if not stopping:
                            raise RuntimeError("adapter owner response could not be delivered")
            return 0
        finally:
            if child is not None:
                stop_child(child)
            if temporary is not None:
                temporary.unlink(missing_ok=True)
            if published and read_state(path) == state:
                path.unlink()


def start() -> int:
    _, _, fingerprint, timeout, _ = configuration()
    path = state_path()
    existing = read_state(path)
    if existing is not None:
        if existing["fingerprint"] != fingerprint:
            raise RuntimeError("adapter configuration changed; stop its verified owner before restarting")
        if request(existing, "status").get("ready") is True:
            print("synchrod adapter is already ready")
            return 0
        raise RuntimeError("adapter owner is not ready")
    log = Path(os.environ["SYNCHROD_ADAPTER_LOG_FILE"]).resolve()
    log.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(log, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    with os.fdopen(descriptor, "ab") as output:
        owner = subprocess.Popen(
            [sys.executable, str(Path(__file__).resolve()), "serve"],
            stdin=subprocess.DEVNULL, stdout=output, stderr=output,
        )
    transferred = False
    try:
        deadline = time.monotonic() + timeout + 5
        while time.monotonic() < deadline:
            if owner.poll() is not None:
                raise RuntimeError("adapter owner exited during startup; inspect its log file")
            state = read_state(path)
            if state is not None and request(state, "status").get("ready") is True:
                transferred = True
                print("synchrod adapter is ready")
                return 0
            time.sleep(0.1)
        raise RuntimeError("adapter startup deadline expired")
    finally:
        if not transferred:
            stop_child(owner, timeout=20)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("operation", choices=("start", "serve", "status", "stop"))
    arguments = parser.parse_args()
    if arguments.operation == "serve":
        return serve()
    if arguments.operation == "start":
        return start()
    state = read_state(state_path())
    if state is None:
        return 3 if arguments.operation == "status" else 0
    if arguments.operation == "status":
        if state["fingerprint"] != configuration()[2]:
            raise RuntimeError("adapter configuration does not match its owner")
        return 0 if request(state, "status").get("ready") is True else 1
    if request(state, "stop").get("stopped") is not True:
        raise RuntimeError("adapter owner did not confirm shutdown")
    print("synchrod adapter stopped")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (RuntimeError, OSError, ValueError) as error:
        print(f"adapter lifecycle: {error}", file=sys.stderr)
        raise SystemExit(1)
