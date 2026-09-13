from __future__ import annotations

import json
from pathlib import Path
import plistlib
import sqlite3
import tempfile
import unittest

from director.inbox import InboxStore
from scripts.restart_receiver import LABEL, RestartError, restart_receiver

MISSING = "Could not find service"


class RestartReceiverTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.project = Path(self.temp.name).resolve()
        (self.project / "config").mkdir()
        self.primary_config = self.project / "config/director.json"
        self.test_config = self.project / "config/director-tests.json"
        self.primary_config.write_text(json.dumps({
            "team_id": "T", "channel_id": "C", "owner_user_id": "U", "enabled": True,
            "database_path": "state/inbox.sqlite3", "additional_configs": ["config/director-tests.json"],
        }))
        self.test_config.write_text(json.dumps({
            "team_id": "T", "channel_id": "TC", "owner_user_id": "U", "enabled": True,
            "environment": "test", "database_path": "state/testing/inbox.sqlite3",
        }))
        self.plist = self.project / "receiver.plist"
        with self.plist.open("wb") as handle:
            plistlib.dump({
                "Label": LABEL,
                "WorkingDirectory": str(self.project),
                "ProgramArguments": [str(self.project / ".venv/bin/python"), "-m", "director", "listen"],
            }, handle)
        self._set_health(self.project / "state/inbox.sqlite3", 101)
        self._set_health(self.project / "state/testing/inbox.sqlite3", 101)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def _set_health(self, path: Path, updated_at: float, *, connected: bool = True) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with InboxStore(path) as store:
            store.set_checkpoint("receiver.loop", json.dumps({"connected": connected}))
            store._connection.execute(
                "UPDATE checkpoints SET updated_at=? WHERE name='receiver.loop'", (updated_at,)
            )
        dispatch = path.parent / "dispatch"
        dispatch.mkdir(exist_ok=True)
        db = sqlite3.connect(dispatch / "jobs.sqlite3")
        try:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS jobs (
                    key TEXT, state TEXT, error_code TEXT, runtime TEXT,
                    agent_group_id INTEGER, finished_at REAL
                );
                CREATE TABLE IF NOT EXISTS guardian_reply_approvals (job_key TEXT, state TEXT);
                CREATE TABLE IF NOT EXISTS agent_reply_authorities (job_key TEXT, state TEXT);
            """)
            db.commit()
        finally:
            db.close()

    def _invoke(self, responses, *, preflight=("print", 0)):
        calls = []

        def invoke(command):
            calls.append(command)
            response = preflight if len(calls) == 1 else responses.pop(0)
            self.assertEqual(response[0], command[1])
            return {"returncode": response[1], "stdout": "", "stderr": response[2] if len(response) > 2 else ""}

        return calls, invoke

    def _restart(self, responses, **kwargs):
        calls, invoke = self._invoke(responses)
        result = restart_receiver(
            self.project, self.primary_config, self.test_config, self.plist,
            uid=501, invoke=invoke, now=lambda: 100, monotonic=lambda: 0,
            sleep=lambda _seconds: None, **kwargs,
        )
        return calls, result

    def _add_spent_guardian_fence(
        self,
        *,
        approval_state: str = "used",
        authority_state: str = "closed",
        job_state: str = "blocked",
        error_code: str = "guardian_approved_retry_unpublished",
        group_id: int = 731,
    ) -> None:
        db = sqlite3.connect(self.project / "state/testing/dispatch/jobs.sqlite3")
        try:
            db.execute(
                "INSERT INTO jobs(state,key,error_code,runtime,agent_group_id,finished_at) VALUES (?,?,?,?,?,?)",
                (job_state, "spent-guardian", error_code, "acp", group_id, 99),
            )
            db.execute(
                "INSERT INTO guardian_reply_approvals(job_key,state) VALUES (?,?)",
                ("spent-guardian", approval_state),
            )
            db.execute(
                "INSERT INTO agent_reply_authorities(job_key,state) VALUES (?,?)",
                ("spent-guardian", authority_state),
            )
            db.commit()
        finally:
            db.close()

    def test_restarts_only_after_teardown_absence_and_fresh_health(self):
        calls, result = self._restart([
            ("bootout", 0), ("print", 0), ("print", 3, MISSING), ("bootstrap", 0),
        ])
        self.assertEqual(result["bootstrap_status"], "first_succeeded")
        self.assertEqual(result["health"], "connected")
        self.assertEqual([command[1] for command in calls], ["print", "bootout", "print", "print", "bootstrap"])
        self.assertEqual(calls[-1][0:3], ["launchctl", "bootstrap", "gui/501"])


    def test_does_not_bootstrap_until_teardown_is_confirmed(self):
        calls, invoke = self._invoke([("bootout", 0), ("print", 0), ("print", 0)])
        with self.assertRaises(RestartError) as raised:
            restart_receiver(
                self.project, self.primary_config, self.test_config, self.plist,
                uid=501, invoke=invoke, now=lambda: 100, monotonic=lambda: 0,
                sleep=lambda _seconds: None, teardown_timeout_seconds=0,
            )
        self.assertEqual(raised.exception.code, "teardown_not_confirmed")
        self.assertEqual([command[1] for command in calls], ["print", "bootout", "print", "print"])

    def test_preserves_failed_bootstrap_and_retries_once_after_proven_absence(self):
        calls, result = self._restart([
            ("bootout", 0), ("print", 3, MISSING), ("bootstrap", 5, "Input/output error"),
            ("print", 3, MISSING), ("bootstrap", 0),
        ])
        self.assertEqual(result["bootstrap_status"], "recovered_after_absence_retry")
        self.assertEqual(result["first_bootstrap"]["returncode"], 5)
        self.assertEqual(result["first_bootstrap"]["stderr"], "Input/output error")
        self.assertEqual([command[1] for command in calls].count("bootstrap"), 2)

    def test_does_not_retry_failed_bootstrap_while_service_is_present(self):
        calls, invoke = self._invoke([
            ("bootout", 0), ("print", 3, MISSING), ("bootstrap", 5, "Input/output error"),
            ("print", 0), ("print", 0),
        ])
        with self.assertRaises(RestartError) as raised:
            restart_receiver(
                self.project, self.primary_config, self.test_config, self.plist,
                uid=501, invoke=invoke, now=lambda: 100, monotonic=lambda: 0,
                sleep=lambda _seconds: None, teardown_timeout_seconds=0,
            )
        self.assertEqual(raised.exception.code, "bootstrap_failed_service_present")
        self.assertEqual(raised.exception.result["first_bootstrap"]["returncode"], 5)
        self.assertEqual([command[1] for command in calls].count("bootstrap"), 1)

    def test_fails_when_bootstrap_does_not_produce_fresh_connected_health(self):
        self._set_health(self.project / "state/testing/inbox.sqlite3", 99, connected=False)
        calls, invoke = self._invoke([
            ("bootout", 0), ("print", 3, MISSING), ("bootstrap", 0),
        ])
        with self.assertRaises(RestartError) as raised:
            restart_receiver(
                self.project, self.primary_config, self.test_config, self.plist,
                uid=501, invoke=invoke, now=lambda: 100, monotonic=lambda: 0,
                sleep=lambda _seconds: None, health_timeout_seconds=0,
            )
        self.assertEqual(raised.exception.code, "receiver_health_not_recovered")
        self.assertEqual(raised.exception.result["bootstrap_status"], "first_succeeded")
        self.assertEqual(raised.exception.result["health"], "not_fresh_connected")


    def test_attempts_finally_restoration_if_an_error_precedes_bootstrap(self):
        calls, invoke = self._invoke([
            ("bootout", 0), ("print", 3, MISSING), ("print", 3, MISSING), ("bootstrap", 0),
        ])
        with self.assertRaises(RestartError) as raised:
            restart_receiver(
                self.project, self.primary_config, self.test_config, self.plist,
                uid=501, invoke=invoke,
                now=lambda: (_ for _ in ()).throw(RuntimeError("clock failed")),
                monotonic=lambda: 0, sleep=lambda _seconds: None,
            )
        self.assertEqual(raised.exception.code, "restart_operation_failed")
        self.assertEqual(raised.exception.result["restoration"], "succeeded_after_failure")
        self.assertEqual([command[1] for command in calls], ["print", "bootout", "print", "print", "bootstrap"])

    def test_refuses_missing_dispatch_state_before_launchctl_control(self):
        (self.project / "state/testing/dispatch/jobs.sqlite3").unlink()
        calls, invoke = self._invoke([])
        with self.assertRaisesRegex(FileNotFoundError, "dispatch_state_missing"):
            restart_receiver(
                self.project, self.primary_config, self.test_config, self.plist,
                uid=501, invoke=invoke, now=lambda: 100, monotonic=lambda: 0,
                sleep=lambda _seconds: None,
            )
        self.assertEqual(calls, [])



    def test_bootout_timeout_reprobes_and_restores_only_after_absence(self):
        calls, invoke = self._invoke([
            ("bootout", None, "launchctl command timed out"),
            ("print", 3, MISSING),
            ("bootstrap", 0),
        ])
        with self.assertRaises(RestartError) as raised:
            restart_receiver(
                self.project, self.primary_config, self.test_config, self.plist,
                uid=501, invoke=invoke, now=lambda: 100, monotonic=lambda: 0,
                sleep=lambda _seconds: None,
            )
        self.assertEqual(raised.exception.code, "bootout_timeout_uncertain")
        self.assertEqual(raised.exception.result["restoration"], "succeeded_after_failure")
        self.assertEqual([command[1] for command in calls], ["print", "bootout", "print", "bootstrap"])

    def test_rechecks_absence_and_restores_after_launchctl_print_exception(self):
        calls = []
        responses = iter([
            {"returncode": 0, "stdout": "", "stderr": ""},
            {"returncode": 0, "stdout": "", "stderr": ""},
            RuntimeError("launchctl unavailable"),
            {"returncode": 3, "stdout": "", "stderr": MISSING},
            {"returncode": 0, "stdout": "", "stderr": ""},
        ])

        def invoke(command):
            calls.append(command)
            response = next(responses)
            if isinstance(response, Exception):
                raise response
            return response

        with self.assertRaises(RestartError) as raised:
            restart_receiver(
                self.project, self.primary_config, self.test_config, self.plist,
                uid=501, invoke=invoke, now=lambda: 100, monotonic=lambda: 0,
                sleep=lambda _seconds: None,
            )
        self.assertEqual(raised.exception.code, "restart_operation_failed")
        self.assertEqual(raised.exception.result["restoration"], "succeeded_after_failure")
        self.assertEqual([command[1] for command in calls], ["print", "bootout", "print", "print", "bootstrap"])

    def test_refuses_unknown_launchctl_print_error_before_bootstrap(self):
        calls, invoke = self._invoke([("bootout", 0), ("print", 1, "permission denied"), ("print", 1, "permission denied")])
        with self.assertRaises(RestartError) as raised:
            restart_receiver(
                self.project, self.primary_config, self.test_config, self.plist,
                uid=501, invoke=invoke, now=lambda: 100, monotonic=lambda: 0,
                sleep=lambda _seconds: None,
            )
        self.assertEqual(raised.exception.code, "teardown_state_unknown")
        self.assertEqual([command[1] for command in calls], ["print", "bootout", "print", "print"])


    def test_nonzero_bootout_reprobes_and_restores_after_absence(self):
        calls, invoke = self._invoke([
            ("bootout", 5, "Input/output error"),
            ("print", 3, MISSING),
            ("bootstrap", 0),
        ])
        with self.assertRaises(RestartError) as raised:
            restart_receiver(
                self.project, self.primary_config, self.test_config, self.plist,
                uid=501, invoke=invoke, now=lambda: 100, monotonic=lambda: 0,
                sleep=lambda _seconds: None,
            )
        self.assertEqual(raised.exception.code, "bootout_failed")
        self.assertEqual(raised.exception.result["restoration"], "succeeded_after_failure")
        self.assertEqual([command[1] for command in calls], ["print", "bootout", "print", "bootstrap"])

    def test_nonzero_bootout_does_not_bootstrap_while_service_stays_present(self):
        calls, invoke = self._invoke([
            ("bootout", 5, "Input/output error"),
            ("print", 0),
        ])
        with self.assertRaises(RestartError) as raised:
            restart_receiver(
                self.project, self.primary_config, self.test_config, self.plist,
                uid=501, invoke=invoke, now=lambda: 100, monotonic=lambda: 0,
                sleep=lambda _seconds: None, teardown_timeout_seconds=0,
            )
        self.assertEqual(raised.exception.code, "bootout_failed")
        self.assertEqual(raised.exception.result["restoration"], "not_attempted_service_present")
        self.assertEqual([command[1] for command in calls], ["print", "bootout", "print"])

    def test_refuses_initially_unloaded_service_without_bootout_or_bootstrap(self):
        calls, invoke = self._invoke([], preflight=("print", 3, MISSING))
        with self.assertRaises(RestartError) as raised:
            restart_receiver(
                self.project, self.primary_config, self.test_config, self.plist,
                uid=501, invoke=invoke, now=lambda: 100, monotonic=lambda: 0,
                sleep=lambda _seconds: None,
            )
        self.assertEqual(raised.exception.code, "receiver_not_loaded")
        self.assertEqual([command[1] for command in calls], ["print"])

    def test_refuses_active_dispatch_work_before_launchctl_control(self):
        db = sqlite3.connect(self.project / "state/testing/dispatch/jobs.sqlite3")
        try:
            db.execute("INSERT INTO jobs(state) VALUES ('running')")
            db.commit()
        finally:
            db.close()
        calls, invoke = self._invoke([])
        with self.assertRaises(RestartError) as raised:
            restart_receiver(
                self.project, self.primary_config, self.test_config, self.plist,
                uid=501, invoke=invoke, now=lambda: 100, monotonic=lambda: 0,
                sleep=lambda _seconds: None,
            )
        self.assertEqual(raised.exception.code, "active_dispatch_work")
        self.assertEqual(calls, [])

    def test_allows_exactly_one_spent_guardian_fence_after_owned_group_release(self):
        self._add_spent_guardian_fence()
        db = sqlite3.connect(self.project / "state/testing/dispatch/jobs.sqlite3")
        try:
            before = (
                db.execute("SELECT state,error_code,runtime,agent_group_id,finished_at FROM jobs WHERE key='spent-guardian'").fetchone(),
                db.execute("SELECT state FROM guardian_reply_approvals WHERE job_key='spent-guardian'").fetchone(),
                db.execute("SELECT state FROM agent_reply_authorities WHERE job_key='spent-guardian'").fetchone(),
            )
        finally:
            db.close()
        observed_groups = []
        calls, result = self._restart([
            ("bootout", 0), ("print", 3, MISSING), ("bootstrap", 0),
        ], runtime_group_gone=lambda group: observed_groups.append(group) is None)
        db = sqlite3.connect(self.project / "state/testing/dispatch/jobs.sqlite3")
        try:
            after = (
                db.execute("SELECT state,error_code,runtime,agent_group_id,finished_at FROM jobs WHERE key='spent-guardian'").fetchone(),
                db.execute("SELECT state FROM guardian_reply_approvals WHERE job_key='spent-guardian'").fetchone(),
                db.execute("SELECT state FROM agent_reply_authorities WHERE job_key='spent-guardian'").fetchone(),
            )
        finally:
            db.close()
        self.assertEqual(observed_groups, [731])
        self.assertEqual(after, before)
        self.assertTrue(result["spent_guardian_maintenance"])
        self.assertEqual(result["spent_guardian_runtime"], "gone")
        self.assertEqual([command[1] for command in calls], ["print", "bootout", "print", "bootstrap"])

    def test_refuses_spent_guardian_fence_with_active_authority_before_launchctl(self):
        self._add_spent_guardian_fence(authority_state="active")
        calls, invoke = self._invoke([])
        with self.assertRaises(RestartError) as raised:
            restart_receiver(
                self.project, self.primary_config, self.test_config, self.plist,
                uid=501, invoke=invoke, now=lambda: 100, monotonic=lambda: 0,
                sleep=lambda _seconds: None,
            )
        self.assertEqual(raised.exception.code, "active_dispatch_work")
        self.assertEqual(calls, [])

    def test_refuses_spent_guardian_fence_with_unresolved_approval_before_launchctl(self):
        self._add_spent_guardian_fence(approval_state="uncertain")
        calls, invoke = self._invoke([])
        with self.assertRaises(RestartError) as raised:
            restart_receiver(
                self.project, self.primary_config, self.test_config, self.plist,
                uid=501, invoke=invoke, now=lambda: 100, monotonic=lambda: 0,
                sleep=lambda _seconds: None,
            )
        self.assertEqual(raised.exception.code, "active_dispatch_work")
        self.assertEqual(calls, [])

    def test_refuses_unexpected_blocked_state_before_launchctl(self):
        self._add_spent_guardian_fence(error_code="acp_timeout_uncertain")
        calls, invoke = self._invoke([])
        with self.assertRaises(RestartError) as raised:
            restart_receiver(
                self.project, self.primary_config, self.test_config, self.plist,
                uid=501, invoke=invoke, now=lambda: 100, monotonic=lambda: 0,
                sleep=lambda _seconds: None,
            )
        self.assertEqual(raised.exception.code, "active_dispatch_work")
        self.assertEqual(calls, [])

    def test_refuses_spent_guardian_fence_alongside_other_active_work(self):
        self._add_spent_guardian_fence()
        db = sqlite3.connect(self.project / "state/testing/dispatch/jobs.sqlite3")
        try:
            db.execute("INSERT INTO jobs(state,key) VALUES ('pending','other-work')")
            db.commit()
        finally:
            db.close()
        calls, invoke = self._invoke([])
        with self.assertRaises(RestartError) as raised:
            restart_receiver(
                self.project, self.primary_config, self.test_config, self.plist,
                uid=501, invoke=invoke, now=lambda: 100, monotonic=lambda: 0,
                sleep=lambda _seconds: None,
            )
        self.assertEqual(raised.exception.code, "active_dispatch_work")
        self.assertEqual(calls, [])

    def test_refuses_spent_guardian_fence_when_another_approval_is_pending(self):
        self._add_spent_guardian_fence()
        db = sqlite3.connect(self.project / "state/testing/dispatch/jobs.sqlite3")
        try:
            db.execute(
                "INSERT INTO guardian_reply_approvals(job_key,state) VALUES (?,?)",
                ("other-record", "pending"),
            )
            db.commit()
        finally:
            db.close()
        calls, invoke = self._invoke([])
        with self.assertRaises(RestartError) as raised:
            restart_receiver(
                self.project, self.primary_config, self.test_config, self.plist,
                uid=501, invoke=invoke, now=lambda: 100, monotonic=lambda: 0,
                sleep=lambda _seconds: None,
            )
        self.assertEqual(raised.exception.code, "active_dispatch_work")
        self.assertEqual(calls, [])

    def test_restores_service_when_spent_guardian_group_remains_unresolved(self):
        self._add_spent_guardian_fence()
        calls, invoke = self._invoke([
            ("bootout", 0), ("print", 3, MISSING), ("print", 3, MISSING), ("bootstrap", 0),
        ])
        with self.assertRaises(RestartError) as raised:
            restart_receiver(
                self.project, self.primary_config, self.test_config, self.plist,
                uid=501, invoke=invoke, runtime_group_gone=lambda _group: False,
                now=lambda: 100, monotonic=lambda: 0, sleep=lambda _seconds: None,
            )
        self.assertEqual(raised.exception.code, "spent_guardian_runtime_unresolved")
        self.assertEqual(raised.exception.result["spent_guardian_runtime"], "unresolved")
        self.assertEqual(raised.exception.result["restoration"], "succeeded_after_failure")
        self.assertEqual([command[1] for command in calls], ["print", "bootout", "print", "print", "bootstrap"])


if __name__ == "__main__":
    unittest.main()
