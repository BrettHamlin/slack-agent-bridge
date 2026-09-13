"""Safely restart the one shared Director LaunchAgent during a maintenance window.

This tool never reads credentials or starts another Socket Mode connection.  It is
intentionally limited to Director's installed receiver and validates both the
personal and test dispatch stores before changing launchd state.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import plistlib
import sqlite3
import subprocess
import sys
import time
from typing import Callable

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from director.__main__ import load_config
from director.runtime import database_path

LABEL = "com.example.director.receiver"
ACTIVE_JOB_STATES = ("running", "preparing", "pending", "retry", "blocked")
COMMAND_TIMEOUT_SECONDS = 30
ABSENT_MARKERS = ("could not find service", "no such process")


class RestartError(RuntimeError):
    """A restart could not be proved safe or restored."""

    def __init__(self, code: str, result: dict):
        super().__init__(code)
        self.code = code
        self.result = result


def _command_result(completed: subprocess.CompletedProcess[str]) -> dict:
    """Keep lifecycle evidence bounded and avoid environment disclosure."""
    return {
        "returncode": completed.returncode,
        "stdout": (completed.stdout or "")[-2000:],
        "stderr": (completed.stderr or "")[-2000:],
    }


def _run(command: list[str]) -> dict:
    try:
        completed = subprocess.run(
            command,
            check=False,
            text=True,
            capture_output=True,
            timeout=COMMAND_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired:
        return {"returncode": None, "stdout": "", "stderr": "launchctl command timed out"}
    return _command_result(completed)


def _dispatch_counts(project: Path, config: dict) -> dict[str, int]:
    state = database_path(config, project).resolve().parent / "dispatch" / "jobs.sqlite3"
    if not state.exists():
        raise FileNotFoundError("dispatch_state_missing")
    uri = f"file:{state}?mode=ro"
    connection = sqlite3.connect(uri, uri=True)
    try:
        return dict(connection.execute("SELECT state, count(*) FROM jobs GROUP BY state"))
    finally:
        connection.close()


def _dispatch_path(project: Path, config: dict) -> Path:
    return database_path(config, project).resolve().parent / "dispatch" / "jobs.sqlite3"


def _spent_guardian_maintenance_group(project: Path, configs: tuple[dict, dict]) -> int | None:
    """Return the sole safe spent-approval group, otherwise retain the active fence.

    This is read-only preflight evidence.  It deliberately recognizes one
    terminal Guardian retry only; it cannot make a pending approval restartable
    or relax any other blocked work.
    """
    active_jobs: list[tuple[sqlite3.Connection, sqlite3.Row]] = []
    connections: list[sqlite3.Connection] = []
    try:
        for config in configs:
            state = _dispatch_path(project, config)
            uri = f"file:{state}?mode=ro"
            connection = sqlite3.connect(uri, uri=True)
            connection.row_factory = sqlite3.Row
            connections.append(connection)
            try:
                active_jobs.extend((connection, row) for row in connection.execute(
                    "SELECT key,state,error_code,runtime,agent_group_id,finished_at FROM jobs "
                    "WHERE state IN ('running','preparing','pending','retry','blocked')"
                ).fetchall())
            except sqlite3.OperationalError:
                return None

        if len(active_jobs) != 1:
            return None
        connection, job = active_jobs[0]
        if (job['state'], job['error_code'], job['runtime']) != (
            'blocked', 'guardian_approved_retry_unpublished', 'acp'
        ) or job['finished_at'] is None:
            return None
        try:
            group_id = int(job['agent_group_id'])
        except (TypeError, ValueError):
            return None
        if group_id <= 0:
            return None

        try:
            approval_states = [row[0] for row in connection.execute(
                "SELECT state FROM guardian_reply_approvals WHERE job_key=?", (job['key'],)
            )]
            authority_states = [row[0] for row in connection.execute(
                "SELECT state FROM agent_reply_authorities WHERE job_key=?", (job['key'],)
            )]
            unresolved_approval = any(
                other.execute(
                    "SELECT 1 FROM guardian_reply_approvals "
                    "WHERE state IN ('pending','submitting','uncertain') LIMIT 1"
                ).fetchone()
                for other in connections
            )
        except sqlite3.OperationalError:
            return None
        if approval_states != ['used'] or not authority_states or any(
            state != 'closed' for state in authority_states
        ) or unresolved_approval:
            return None
        return group_id
    finally:
        for connection in connections:
            connection.close()


def _runtime_group_gone(group_id: int) -> bool:
    """Return true only when the owned group is absent; never signal it."""
    try:
        os.killpg(group_id, 0)
    except ProcessLookupError:
        return True
    except (PermissionError, ValueError, TypeError):
        return False
    return False


def _checkpoint(path: Path, name: str) -> tuple[str | None, float | None]:
    if not path.exists():
        return None, None
    uri = f"file:{path}?mode=ro"
    connection = sqlite3.connect(uri, uri=True)
    try:
        row = connection.execute(
            "SELECT value, updated_at FROM checkpoints WHERE name=?", (name,)
        ).fetchone()
    finally:
        connection.close()
    return (None, None) if row is None else (row[0], float(row[1]))


def _validate_plist(plist: Path, project: Path) -> None:
    values = plistlib.loads(plist.read_bytes())
    expected = [str(project / ".venv/bin/python"), "-m", "director", "listen"]
    if (
        values.get("Label") != LABEL
        or values.get("WorkingDirectory") != str(project)
        or values.get("ProgramArguments") != expected
    ):
        raise ValueError("unexpected_director_receiver_plist")


def _service_name(uid: int) -> str:
    return f"gui/{uid}/{LABEL}"


def _print_evidence(result: dict) -> dict:
    return {"returncode": result.get("returncode"), "stderr": result.get("stderr", "")[-2000:]}


def _print_state(result: dict) -> str:
    if result["returncode"] == 0:
        return "present"
    text = f'{result.get("stdout", "")}\n{result.get("stderr", "")}'.lower()
    if any(marker in text for marker in ABSENT_MARKERS):
        return "absent"
    return "unknown"


def _wait_for_absence(
    service: str,
    timeout_seconds: float,
    poll_seconds: float,
    invoke: Callable[[list[str]], dict],
    monotonic: Callable[[], float],
    sleep: Callable[[float], None],
) -> tuple[str, int, dict]:
    deadline = monotonic() + timeout_seconds
    polls = 0
    while True:
        polls += 1
        observed = invoke(["launchctl", "print", service])
        state = _print_state(observed)
        if state != "present":
            return state, polls, observed
        if monotonic() >= deadline:
            return "present", polls, observed
        sleep(poll_seconds)


def _loop_is_fresh_and_connected(path: Path, started_at: float) -> bool:
    value, updated_at = _checkpoint(path, "receiver.loop")
    _stopped, stopped_at = _checkpoint(path, "receiver.stopped")
    if value is None or updated_at is None or updated_at < started_at:
        return False
    try:
        connected = json.loads(value).get("connected") is True
    except json.JSONDecodeError:
        return False
    return connected and (stopped_at is None or stopped_at < updated_at)


def _wait_for_health(
    paths: tuple[Path, Path],
    started_at: float,
    timeout_seconds: float,
    poll_seconds: float,
    monotonic: Callable[[], float],
    sleep: Callable[[float], None],
) -> bool:
    deadline = monotonic() + timeout_seconds
    while True:
        if all(_loop_is_fresh_and_connected(path, started_at) for path in paths):
            return True
        if monotonic() >= deadline:
            return False
        sleep(poll_seconds)


def restart_receiver(
    project: Path,
    primary_config: Path,
    test_config: Path,
    plist: Path,
    *,
    teardown_timeout_seconds: float = 15,
    health_timeout_seconds: float = 120,
    poll_seconds: float = 0.25,
    uid: int | None = None,
    invoke: Callable[[list[str]], dict] = _run,
    runtime_group_gone: Callable[[int], bool] = _runtime_group_gone,
    now: Callable[[], float] = time.time,
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> dict:
    """Restart the exact receiver, retaining all first-attempt evidence.

    A failed bootstrap is retried exactly once only after launchd has again
    proved the service absent.  A successful bootstrap is still a failure until
    each configured channel records a fresh connected receiver loop.
    """
    project = project.resolve()
    primary = load_config(primary_config)
    test = load_config(test_config)
    if not primary["enabled"] or not test["enabled"]:
        raise ValueError("receiver_or_test_environment_is_disabled")
    if test.get("environment") != "test" or test_config.resolve() not in {
        (project / item).resolve() for item in primary.get("additional_configs", [])
    }:
        raise ValueError("test_config_is_not_registered_with_primary_receiver")
    _validate_plist(plist, project)

    counts = {"primary": _dispatch_counts(project, primary), "test": _dispatch_counts(project, test)}
    active = {
        name: {state: count for state, count in states.items() if state in ACTIVE_JOB_STATES and count}
        for name, states in counts.items()
    }
    maintenance_group = None
    if any(active.values()):
        maintenance_group = _spent_guardian_maintenance_group(project, (primary, test))
    if any(active.values()) and maintenance_group is None:
        raise RestartError("active_dispatch_work", {"job_counts": counts, "active_job_counts": active})

    service = _service_name(os.getuid() if uid is None else uid)
    result = {
        "service": service,
        "plist": str(plist),
        "job_counts": counts,
        "preflight_print": None,
        "bootout": None,
        "teardown": None,
        "first_bootstrap": None,
        "retry_bootstrap": None,
        "bootstrap_status": "not_started",
        "health": "not_checked",
        "restoration": "not_needed",
        "spent_guardian_maintenance": maintenance_group is not None,
    }
    domain = service.rsplit("/", 1)[0]
    bootstrap_command = ["launchctl", "bootstrap", domain, str(plist)]
    bootout_succeeded = False
    bootout_uncertain = False
    bootstrap_confirmed = False
    bootstrap_attempts = 0
    try:
        preflight = invoke(["launchctl", "print", service])
        result["preflight_print"] = _print_evidence(preflight)
        if _print_state(preflight) != "present":
            raise RestartError("receiver_not_loaded", result)

        # Any attempted bootout can race with launchd teardown, even when its
        # immediate result is nonzero. Preserve that result and use the finally
        # probe to restore only if the service can later be proved absent.
        bootout_uncertain = True
        result["bootout"] = invoke(["launchctl", "bootout", service])
        if result["bootout"]["returncode"] is None:
            raise RestartError("bootout_timeout_uncertain", result)
        if result["bootout"]["returncode"] != 0:
            raise RestartError("bootout_failed", result)
        bootout_succeeded = True
        bootout_uncertain = False

        teardown, polls, observed = _wait_for_absence(
            service, teardown_timeout_seconds, poll_seconds, invoke, monotonic, sleep
        )
        result["teardown"] = {"state": teardown, "polls": polls, "last_print": _print_evidence(observed)}
        if teardown == "unknown":
            raise RestartError("teardown_state_unknown", result)
        if teardown != "absent":
            raise RestartError("teardown_not_confirmed", result)
        if maintenance_group is not None:
            result["spent_guardian_runtime"] = "gone" if runtime_group_gone(maintenance_group) else "unresolved"
            if result["spent_guardian_runtime"] != "gone":
                raise RestartError("spent_guardian_runtime_unresolved", result)
        started_at = now()
        bootstrap_attempts += 1
        result["first_bootstrap"] = invoke(bootstrap_command)
        if result["first_bootstrap"]["returncode"] == 0:
            result["bootstrap_status"] = "first_succeeded"
            bootstrap_confirmed = True
        else:
            teardown, polls, observed = _wait_for_absence(
                service, teardown_timeout_seconds, poll_seconds, invoke, monotonic, sleep
            )
            result["retry_teardown"] = {"state": teardown, "polls": polls, "last_print": _print_evidence(observed)}
            if teardown == "unknown":
                raise RestartError("bootstrap_failed_service_unknown", result)
            if teardown != "absent":
                raise RestartError("bootstrap_failed_service_present", result)
            started_at = now()
            bootstrap_attempts += 1
            result["retry_bootstrap"] = invoke(bootstrap_command)
            if result["retry_bootstrap"]["returncode"] != 0:
                raise RestartError("bootstrap_retry_failed", result)
            result["bootstrap_status"] = "recovered_after_absence_retry"
            bootstrap_confirmed = True

        paths = (database_path(primary, project), database_path(test, project))
        result["health"] = "connected" if _wait_for_health(
            paths, started_at, health_timeout_seconds, poll_seconds, monotonic, sleep
        ) else "not_fresh_connected"
        if result["health"] != "connected":
            raise RestartError("receiver_health_not_recovered", result)
        return result
    except RestartError:
        raise
    except Exception as error:
        result["operation_error"] = type(error).__name__
        raise RestartError("restart_operation_failed", result) from error
    finally:
        # After a successful bootout, restoration is mandatory.  Only a fresh,
        # explicit service-absent observation permits the remaining bounded
        # bootstrap attempt; an unknown or present state is never guessed.
        if (bootout_succeeded or bootout_uncertain) and not bootstrap_confirmed and bootstrap_attempts < 2:
            try:
                teardown, polls, observed = _wait_for_absence(
                    service, teardown_timeout_seconds, poll_seconds, invoke, monotonic, sleep
                )
                result["restoration_teardown"] = {
                    "state": teardown,
                    "polls": polls,
                    "last_print": _print_evidence(observed),
                }
                if teardown == "absent":
                    bootstrap_attempts += 1
                    result["restoration"] = "attempted_after_failure"
                    result["restoration_bootstrap"] = invoke(bootstrap_command)
                    bootstrap_confirmed = result["restoration_bootstrap"]["returncode"] == 0
                    if bootstrap_confirmed:
                        result["restoration"] = "succeeded_after_failure"
                else:
                    result["restoration"] = f"not_attempted_service_{teardown}"
            except Exception as error:
                result["restoration"] = "probe_failed"
                result["restoration_error"] = type(error).__name__


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=Path("config/director.json"))
    parser.add_argument("--test-config", type=Path, default=Path("config/director-tests.json"))
    parser.add_argument(
        "--plist",
        type=Path,
        default=Path.home() / "Library/LaunchAgents" / f"{LABEL}.plist",
    )
    parser.add_argument("--teardown-timeout-seconds", type=float, default=15)
    parser.add_argument("--health-timeout-seconds", type=float, default=120)
    parser.add_argument("--poll-seconds", type=float, default=0.25)
    args = parser.parse_args(argv)
    project = args.config.resolve().parent.parent
    try:
        result = restart_receiver(
            project,
            args.config.resolve(),
            args.test_config.resolve(),
            args.plist.resolve(),
            teardown_timeout_seconds=args.teardown_timeout_seconds,
            health_timeout_seconds=args.health_timeout_seconds,
            poll_seconds=args.poll_seconds,
        )
    except RestartError as error:
        print(json.dumps({"ok": False, "error_code": error.code, "result": error.result}))
        return 1
    except Exception as error:
        print(json.dumps({"ok": False, "error_code": type(error).__name__}))
        return 1
    print(json.dumps({"ok": True, "result": result}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
