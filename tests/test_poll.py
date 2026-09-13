from __future__ import annotations

from contextlib import redirect_stdout
import io
import json
from pathlib import Path
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

from director.__main__ import main
from director.inbox import InboxStore, InboundPointer
from director.poll import collect_poll
from director.reminders import ReminderStore
from director.responsibilities import ResponsibilityStore


def pointer(event_id: str, source_ts: str) -> InboundPointer:
    return InboundPointer(
        event_id=event_id,
        source_team_id="T-test",
        source_channel_id="C-test",
        source_ts=source_ts,
        event_ts=source_ts,
        thread_ts=source_ts,
    )


class PollTests(unittest.TestCase):
    def test_manager_and_relay_get_role_specific_actionable_references_without_prose(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "director.sqlite3"
            now = time.time()
            with InboxStore(path) as inbox:
                relayed = inbox.ingest(pointer("event-relayed", "1710000000.000100")).message
                lease = inbox.claim_pending_relay("test", 30)[0]
                inbox.acknowledge_relay(lease)
                unrelayed = inbox.ingest(pointer("event-unrelayed", "1710000001.000100")).message
                completed = inbox.ingest(pointer("event-completed", "1710000002.000100")).message
                inbox.mark_completed(completed.id)
                inbox.set_checkpoint("receiver.loop", '{"connected": true}', now=now)
                inbox.set_checkpoint("luna.scan", str(now - 901), now=now - 901)
                inbox.set_checkpoint("runtime.credentials", "unavailable", now=now - 10)
                inbox.set_checkpoint("receiver.card_render_error", "SlackServiceError", now=now - 5)
            with ResponsibilityStore(path) as responsibilities:
                responsibilities.create("runnable", "DO NOT PRINT OUTCOME", "DO NOT PRINT ACTION", "https://director.test/thread", state="runnable")
                responsibilities.create("expired", "DO NOT PRINT EXPIRED", "DO NOT PRINT ACTION", "https://director.test/thread", state="runnable")
                responsibilities.claim("expired", "worker", lease_seconds=1, now=now - 2)
            with ReminderStore(path) as reminders:
                reminders.create("due", "DO NOT PRINT REMINDER", now - 1)

            manager = collect_poll(path, role="manager", now=now)
            relay = collect_poll(path, role="relay", now=now)

            self.assertEqual(manager["outcome"], "actionable")
            self.assertEqual({item["message_id"] for item in manager["sources"]}, {unrelayed.id, relayed.id})
            self.assertEqual({item["action"] for item in manager["sources"]}, {"relay", "review"})
            self.assertEqual({item["message_id"] for item in relay["sources"]}, {unrelayed.id})
            self.assertEqual([item["id"] for item in manager["reminders"]], ["due"])
            self.assertEqual({item["id"] for item in manager["responsibilities"]}, {"runnable", "expired"})
            self.assertIn("expired_claim", {item["state"] for item in manager["responsibilities"]})
            self.assertIn("credentials_backoff", {item["code"] for item in manager["health"]})
            self.assertIn("card_render_error", {item["code"] for item in manager["health"]})
            self.assertNotIn("recovery_due", {item["code"] for item in manager["health"]})
            self.assertNotIn("recovery_due", {item["code"] for item in relay["health"]})
            rendered = json.dumps(manager)
            self.assertNotIn("DO NOT PRINT", rendered)
            self.assertNotIn(str(completed.id), {str(item["message_id"]) for item in manager["sources"]})

    def test_idle_poll_is_compact_and_has_no_health_or_action_items(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "director.sqlite3"
            now = time.time()
            with InboxStore(path) as inbox:
                inbox.set_checkpoint("receiver.loop", '{"connected": true}', now=now)
                inbox.set_checkpoint("luna.scan", str(now), now=now)
                inbox.set_checkpoint("runtime.credentials", "available", now=now)

            result = collect_poll(path, role="manager", now=now)

            self.assertEqual(result["outcome"], "idle")
            self.assertEqual(result["sources"], [])
            self.assertEqual(result["responsibilities"], [])
            self.assertEqual(result["reminders"], [])
            self.assertEqual(result["health"], [])
            self.assertEqual(result["counters"], {
                "sources": 0,
                "sources_relay": 0,
                "sources_review": 0,
                "responsibilities": 0,
                "responsibilities_expired_claim": 0,
                "reminders_due": 0,
                "health": 0,
                "health_actionable": 0,
            })

    def test_credential_backoff_is_visible_but_does_not_wake_an_idle_role(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "director.sqlite3"
            now = time.time()
            with InboxStore(path) as inbox:
                inbox.set_checkpoint("receiver.loop", '{"connected": true}', now=now)
                inbox.set_checkpoint("luna.scan", str(now), now=now)
                inbox.set_checkpoint("runtime.credentials", "unavailable", now=now - 1)

            result = collect_poll(path, role="relay", now=now)

            self.assertEqual(result["outcome"], "idle")
            self.assertEqual(result["health"], [{"code": "credentials_backoff", "updated_at": now - 1}])
            self.assertEqual(result["counters"]["health_actionable"], 0)
            self.assertNotIn("recovery_due", {item["code"] for item in result["health"]})

    def test_manager_reports_a_stale_recovery_scan_after_twenty_minutes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "director.sqlite3"
            now = time.time()
            with InboxStore(path) as inbox:
                inbox.set_checkpoint("receiver.loop", '{"connected": true}', now=now)
                inbox.set_checkpoint("luna.scan", str(now - 1201), now=now - 1201)
                inbox.set_checkpoint("runtime.credentials", "available", now=now)

            result = collect_poll(path, role="manager", now=now)

            self.assertEqual(result["outcome"], "actionable")
            self.assertEqual(result["health"], [{"code": "recovery_stale", "updated_at": now - 1201}])

    def test_relay_does_not_reap_or_report_an_expired_responsibility_claim(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "director.sqlite3"
            now = time.time()
            with InboxStore(path) as inbox:
                inbox.set_checkpoint("receiver.loop", '{"connected": true}', now=now)
                inbox.set_checkpoint("luna.scan", str(now), now=now)
                inbox.set_checkpoint("runtime.credentials", "available", now=now)
            with ResponsibilityStore(path) as responsibilities:
                responsibilities.create("expired", "private outcome", "private action", "https://director.test/thread", state="runnable")
                responsibilities.claim("expired", "worker", lease_seconds=1, now=now - 2)

            relay = collect_poll(path, role="relay", now=now)
            with ResponsibilityStore(path) as responsibilities:
                self.assertEqual(responsibilities.get("expired").state, "claimed")
            manager = collect_poll(path, role="manager", now=now)

            self.assertEqual(relay["outcome"], "idle")
            self.assertEqual(relay["responsibilities"], [])
            self.assertEqual(manager["responsibilities"][0]["state"], "expired_claim")

    def test_relay_does_not_repeat_a_source_owned_by_an_active_relay_lease(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "director.sqlite3"
            now = time.time()
            with InboxStore(path) as inbox:
                message = inbox.ingest(pointer("event-leased", "1710000003.000100")).message
                inbox.claim_pending_relay("other-relay", 30, now=now)
                inbox.set_checkpoint("receiver.loop", '{"connected": true}', now=now)
                inbox.set_checkpoint("luna.scan", str(now), now=now)
                inbox.set_checkpoint("runtime.credentials", "available", now=now)

            manager = collect_poll(path, role="manager", now=now)
            relay = collect_poll(path, role="relay", now=now)

            self.assertEqual(manager["sources"], [{
                "message_id": message.id,
                "revision": 1,
                "source_ts": "1710000003.000100",
                "thread_ts": "1710000003.000100",
                "action": "relay",
            }])
            self.assertEqual(relay["sources"], [])

    def test_disabled_poll_does_not_open_or_advance_state(self) -> None:
        result = collect_poll(":memory:", role="manager", enabled=False)

        self.assertEqual(result["outcome"], "disabled")
        self.assertEqual(result["sources"], [])
        self.assertEqual(result["responsibilities"], [])
        self.assertEqual(result["counters"]["health_actionable"], 0)

    def test_cli_emits_one_json_poll_result(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            project = Path(directory) / "project"
            config_dir = project / "config"
            config_dir.mkdir(parents=True)
            config = config_dir / "director.json"
            config.write_text(json.dumps({
                "team_id": "T-test",
                "channel_id": "C-test",
                "owner_user_id": "U-test",
                "workspace_domain": "director.test",
                "enabled": True,
                "database_path": "state/director.sqlite3",
            }))
            output = io.StringIO()
            with patch.object(sys, "argv", ["director", "poll", "--role", "relay", "--config", str(config)]), redirect_stdout(output):
                self.assertEqual(main(), 0)

            result = json.loads(output.getvalue())
            self.assertEqual(result["role"], "relay")
            self.assertEqual(result["outcome"], "actionable")
            self.assertIn("receiver_unseen", {item["code"] for item in result["health"]})
            self.assertIn("recovery_due", {item["code"] for item in result["health"]})

    def test_rejects_an_unknown_role(self) -> None:
        with self.assertRaises(ValueError):
            collect_poll(":memory:", role="other")
