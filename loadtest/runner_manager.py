from __future__ import annotations

import json
import os
import signal
import subprocess
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def utc_now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def load_state(state_file: Path) -> dict[str, Any]:
    if not state_file.exists():
        return {"runners": {}}
    data = json.loads(state_file.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        return {"runners": {}}
    data.setdefault("runners", {})
    return data


def save_state(state_file: Path, state: dict[str, Any]) -> None:
    state_file.parent.mkdir(parents=True, exist_ok=True)
    state_file.write_text(json.dumps(state, indent=2), encoding="utf-8")


def is_pid_running(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


def get_runner_status(state_file: Path, runner_name: str) -> dict[str, Any]:
    state = load_state(state_file)
    data = state.get("runners", {}).get(runner_name)
    if not data:
        return {"name": runner_name, "running": False}
    pid = int(data.get("pid", -1))
    running = is_pid_running(pid)
    status = {
        "name": runner_name,
        "running": running,
        "pid": pid,
        "started_at": data.get("started_at"),
        "cmd": data.get("cmd", []),
        "log_file": data.get("log_file"),
    }
    if not running:
        status["stopped_at"] = data.get("stopped_at", utc_now_iso())
    if data.get("last_stop_error"):
        status["last_stop_error"] = data["last_stop_error"]
    return status


def get_all_runner_statuses(state_file: Path) -> list[dict[str, Any]]:
    state = load_state(state_file)
    runners = list(state.get("runners", {}).keys())
    return [get_runner_status(state_file, name) for name in sorted(runners)]


def start_runner(
    state_file: Path,
    runner_name: str,
    cmd: list[str],
    cwd: Path,
    log_file: Path,
    env: dict[str, str] | None = None,
) -> dict[str, Any]:
    status = get_runner_status(state_file, runner_name)
    if status.get("running"):
        raise RuntimeError(f"Runner '{runner_name}' is already running with PID {status['pid']}")

    log_file.parent.mkdir(parents=True, exist_ok=True)
    with log_file.open("ab") as log:
        log.write(f"\n[{utc_now_iso()}] START {' '.join(cmd)}\n".encode("utf-8"))
        process = subprocess.Popen(
            cmd,
            cwd=str(cwd),
            env=env,
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )

    state = load_state(state_file)
    state.setdefault("runners", {})
    state["runners"][runner_name] = {
        "pid": process.pid,
        "started_at": utc_now_iso(),
        "cmd": cmd,
        "log_file": str(log_file),
        "last_stop_error": None,
    }
    save_state(state_file, state)
    return get_runner_status(state_file, runner_name)


def stop_runner(state_file: Path, runner_name: str, force: bool = False) -> dict[str, Any]:
    state = load_state(state_file)
    runner = state.get("runners", {}).get(runner_name)
    if not runner:
        return {"name": runner_name, "running": False}

    pid = int(runner.get("pid", -1))
    running = is_pid_running(pid)
    stop_error: str | None = None

    if running and pid > 0:
        sig = signal.SIGKILL if force else signal.SIGTERM
        try:
            # Preferred for start_new_session=True child processes.
            os.killpg(pid, sig)
        except ProcessLookupError:
            running = False
        except PermissionError as exc:
            # Some environments disallow killpg; fall back to single PID.
            try:
                os.kill(pid, sig)
            except ProcessLookupError:
                running = False
            except PermissionError as inner_exc:
                stop_error = str(inner_exc)
            except OSError as inner_exc:
                stop_error = str(inner_exc)
            else:
                # Fallback succeeded, so keep stop_error clear.
                stop_error = None
        except OSError as exc:
            stop_error = str(exc)

    runner["stopped_at"] = utc_now_iso()
    runner["last_stop_error"] = stop_error
    save_state(state_file, state)
    return get_runner_status(state_file, runner_name)


def read_log_tail(log_file: Path, lines: int = 120) -> str:
    if not log_file.exists():
        return ""
    with log_file.open("r", encoding="utf-8", errors="ignore") as handle:
        return "".join(deque(handle, maxlen=lines))
