"""Focused durable fences for ACP-prepared source publication.

These tests use the real inbox, outbox, Slack service, and dispatcher with a
synthetic Slack API.  They never contact a Slack workspace.
"""

from __future__ import annotations

from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from director.dispatcher import Dispatcher
from director.inbox import InboxStore, InboundPointer
from director.responsibilities import ResponsibilityStore
from director.slack_service import SlackService
from director.slack_transport import SlackAllowlist


ALLOWLIST = SlackAllowlist("T-allowed", "C-allowed", "U-owner", "director.test")
ROOT = "1710000000.000100"
SOURCE = "1710000001.000200"


class FakeSlack:
    """Minimal deterministic Slack surface used by SlackService."""

    def __init__(self) -> None:
        self.posts: list[dict[str, object]] = []
        self.reactions: list[dict[str, object]] = []
        self.reply_messages: list[dict[str, object]] = []
        self.reply_calls = 0
        self.fail_post = False
        self.next_ts = "1720000000.000100"

    def conversations_replies(self, **kwargs: object) -> dict[str, object]:
        self.reply_calls += 1
        return {"ok": True, "channel": "C-allowed", "messages": list(self.reply_messages)}

    def conversations_history(self, **kwargs: object) -> dict[str, object]:
        return {"ok": True, "channel": "C-allowed", "messages": []}

    def reactions_add(self, **kwargs: object) -> dict[str, object]:
        self.reactions.append(kwargs)
        return {"ok": True}

    def chat_postMessage(self, **kwargs: object) -> dict[str, object]:
        self.posts.append(kwargs)
        if self.fail_post:
            raise RuntimeError("synthetic connection loss after request write")
        return {"ok": True, "channel": "C-allowed", "ts": self.next_ts,
                "message": {"ts": self.next_ts}}


class PreparedPublishTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.project = Path(self.temp.name)
        (self.project / "state").mkdir()
        self.inbox = InboxStore(self.project / "state" / "inbox.sqlite3")
        self.slack = FakeSlack()
        self.slack.reply_messages = [
            {"ts": ROOT, "user": "U-owner", "text": "thread root"},
            {"ts": SOURCE, "thread_ts": ROOT, "user": "U-owner", "text": "source request"},
        ]
        # Responsibilities and the outbox share Director's configured database.
        self.service = SlackService(self.slack, self.inbox, ALLOWLIST, self.project / "state" / "inbox.sqlite3")
        self.dispatcher = Dispatcher(self.project, {
            "database_path": "state/inbox.sqlite3",
            "dispatcher": {"enabled": True, "runtime": "acp", "acp": {"command": ["synthetic-acp"]}},
        }, self.inbox, self.service)

    def tearDown(self) -> None:
        self.dispatcher.close()
        self.service.close()
        self.inbox.close()
        self.temp.cleanup()

    def _source(self, *, source_ts: str = SOURCE, event_ts: str = SOURCE) -> object:
        return self.inbox.ingest(InboundPointer(
            "event-" + event_ts, "T-allowed", "C-allowed", source_ts, event_ts, ROOT,
        )).message

    def _active_authority(self) -> tuple[object, str]:
        message = self._source()
        self.dispatcher.enqueue(100)
        job = self.dispatcher.db.execute("SELECT * FROM jobs WHERE message_id=?", (message.id,)).fetchone()
        assert job is not None
        with self.dispatcher.db:
            self.dispatcher.db.execute(
                """UPDATE jobs SET state='running',runtime='acp',agent_session_id=?,
                   agent_generation=?,agent_turn_id=? WHERE key=?""",
                ("session-current", 7, "7:1", job["key"]),
            )
        job = self.dispatcher.db.execute("SELECT * FROM jobs WHERE key=?", (job["key"],)).fetchone()
        assert job is not None
        authority = self.dispatcher._create_publish_authority(job)
        with self.dispatcher.db:
            self.dispatcher.db.execute(
                """UPDATE agent_reply_authorities
                   SET state='active',session_id=?,generation=?,turn_id=? WHERE authority=?""",
                ("session-current", 7, "7:1", authority),
            )
        return job, authority

    def _outbox_state(self, job: object) -> str:
        row = self.service._connection.execute(
            "SELECT state FROM slack_outbox WHERE idempotency_key=?", ("dispatch-answer:" + job["key"],)
        ).fetchone()
        return row["state"] if row else "missing"

    def _linked_responsibility(self, job: object) -> tuple[str, str]:
        """Create one runnable task bound to this exact source revision/root."""
        responsibility_id = "prepared-work"
        with ResponsibilityStore(self.project / "state" / "inbox.sqlite3") as store:
            store.create(
                responsibility_id, "Complete prepared work", "Publish the result",
                "https://director.test/thread/" + ROOT,
                current_thread=ROOT, source_message_id=job["message_id"],
                source_revision=job["revision"], state="runnable",
            )
            claim = store.claim(responsibility_id, "prepared-test")
        return responsibility_id, claim.execution_fence

    def test_current_thread_reply_publishes_once_and_same_text_is_idempotent(self) -> None:
        job, authority = self._active_authority()
        first = self.dispatcher.publish_agent_reply(authority, "prepared answer")
        repeated = self.dispatcher.publish_agent_reply(authority, "prepared answer")

        self.assertEqual(first, {"published": True, "state": "sent"})
        self.assertEqual(repeated, {"published": True, "state": "sent"})
        self.assertEqual(len(self.slack.posts), 1)
        self.assertEqual(self.slack.posts[0]["channel"], "C-allowed")
        self.assertEqual(self.slack.posts[0]["thread_ts"], ROOT)
        self.assertIn(job["revision"], self.inbox.get_receipts(job["message_id"]).completed_revisions)

    def test_published_authority_rejects_different_text(self) -> None:
        _, authority = self._active_authority()
        self.assertTrue(self.dispatcher.publish_agent_reply(authority, "first")["published"])
        result = self.dispatcher.publish_agent_reply(authority, "second")

        self.assertFalse(result["published"])
        self.assertEqual(result["state"], "payload_mismatch")
        self.assertEqual([post["text"] for post in self.slack.posts], ["first"])

    def test_edit_or_delete_rejects_prepared_source_before_send(self) -> None:
        for event_type in ("message_changed", "message_deleted"):
            with self.subTest(event_type=event_type):
                self.tearDown()
                self.setUp()
                _, authority = self._active_authority()
                self.inbox.ingest(InboundPointer(
                    "revision-" + event_type, "T-allowed", "C-allowed", SOURCE,
                    "1710000002.000300", ROOT, event_type,
                ))
                result = self.dispatcher.publish_agent_reply(authority, "must not publish")
                self.assertFalse(result["published"])
                self.assertEqual(result["state"], "source_stale")
                self.assertEqual(self.slack.posts, [])

    def test_previous_turn_authority_cannot_publish_after_turn_changes(self) -> None:
        job, authority = self._active_authority()
        with self.dispatcher.db:
            self.dispatcher.db.execute("UPDATE jobs SET agent_turn_id='7:2' WHERE key=?", (job["key"],))

        self.assertEqual(
            self.dispatcher.publish_agent_reply(authority, "late previous turn"),
            {"published": False, "state": "authority_stale"},
        )
        self.assertEqual(self.slack.posts, [])

    def test_uncertain_send_is_not_replayed(self) -> None:
        job, authority = self._active_authority()
        self.slack.fail_post = True
        first = self.dispatcher.publish_agent_reply(authority, "one uncertain answer")
        second = self.dispatcher.publish_agent_reply(authority, "one uncertain answer")

        self.assertEqual(first, {"published": False, "state": "delivery_uncertain"})
        self.assertEqual(second, {"published": False, "state": "delivery_uncertain"})
        self.assertEqual(len(self.slack.posts), 1)
        self.assertEqual(self._outbox_state(job), "pending_uncertain")
        self.assertNotIn(job["revision"], self.inbox.get_receipts(job["message_id"]).completed_revisions)

    def test_sent_outbox_recovers_completion_without_a_second_send(self) -> None:
        job, authority = self._active_authority()
        original = self.inbox.mark_completed_if_revision
        failed_once = {"value": True}

        def crash_after_send(message_id: int, revision: int, **kwargs: object) -> bool:
            if failed_once["value"]:
                failed_once["value"] = False
                raise RuntimeError("synthetic crash after confirmed Slack response")
            return original(message_id, revision, **kwargs)

        self.inbox.mark_completed_if_revision = crash_after_send  # type: ignore[method-assign]
        with self.assertRaisesRegex(RuntimeError, "after confirmed"):
            self.dispatcher.publish_agent_reply(authority, "recover this")
        self.inbox.mark_completed_if_revision = original  # type: ignore[method-assign]

        recovered = self.dispatcher.publish_agent_reply(authority, "recover this")
        self.assertEqual(recovered, {"published": True, "state": "sent"})
        self.assertEqual(len(self.slack.posts), 1)
        self.assertEqual(self._outbox_state(job), "sent")
        self.assertIn(job["revision"], self.inbox.get_receipts(job["message_id"]).completed_revisions)

    def test_source_turn_responsibility_publishes_once_and_completes_both_records(self) -> None:
        job, authority = self._active_authority()
        responsibility_id, fence = self._linked_responsibility(job)

        result = self.dispatcher.publish_agent_reply(
            authority, "completed fenced work", responsibility_id=responsibility_id, execution_fence=fence,
        )

        self.assertEqual(result, {"published": True, "state": "sent"})
        self.assertEqual(len(self.slack.posts), 1)
        self.assertEqual(self.slack.posts[0]["thread_ts"], ROOT)
        self.assertIn(job["revision"], self.inbox.get_receipts(job["message_id"]).completed_revisions)
        with ResponsibilityStore(self.project / "state" / "inbox.sqlite3") as store:
            self.assertEqual(store.get(responsibility_id).state, "completed")

    def test_source_edit_or_delete_between_fenced_prepare_and_dispatch_never_posts(self) -> None:
        for event_type in ("message_changed", "message_deleted"):
            with self.subTest(event_type=event_type):
                self.tearDown()
                self.setUp()
                job, authority = self._active_authority()
                responsibility_id, fence = self._linked_responsibility(job)
                original_connection = self.service._connection
                fired = {"value": False}

                def revise_after_durable_prepare() -> None:
                    self.inbox.ingest(InboundPointer(
                        "gap-" + event_type, "T-allowed", "C-allowed", SOURCE,
                        "1710000002.000300", ROOT, event_type,
                    ))

                class CommitGapConnection:
                    """Inject the owner revision after phase one, before phase two."""
                    def execute(self, sql: str, *args: object, **kwargs: object) -> object:
                        result = original_connection.execute(sql, *args, **kwargs)
                        if sql == "COMMIT" and not fired["value"]:
                            fired["value"] = True
                            revise_after_durable_prepare()
                        return result

                    def __getattr__(self, name: str) -> object:
                        return getattr(original_connection, name)

                self.service._connection = CommitGapConnection()  # type: ignore[assignment]
                try:
                    result = self.dispatcher.publish_agent_reply(
                        authority, "must not cross a revised source",
                        responsibility_id=responsibility_id, execution_fence=fence,
                    )
                finally:
                    self.service._connection = original_connection

                self.assertTrue(fired["value"])
                self.assertEqual(result, {"published": False, "state": "source_stale"})
                self.assertEqual(self.slack.posts, [])
                self.assertEqual(self._outbox_state(job), "prepared")
                self.assertNotIn(job["revision"], self.inbox.get_receipts(job["message_id"]).completed_revisions)
                with ResponsibilityStore(self.project / "state" / "inbox.sqlite3") as store:
                    self.assertEqual(store.get(responsibility_id).state, "claimed")

    def test_sent_source_work_recovers_its_original_completion_before_source_receipt(self) -> None:
        job, authority = self._active_authority()
        responsibility_id, fence = self._linked_responsibility(job)
        with patch("director.dispatcher.ResponsibilityStore.complete", side_effect=RuntimeError("crash after sent")):
            with self.assertRaisesRegex(RuntimeError, "after sent"):
                self.dispatcher.publish_agent_reply(
                    authority, "recover the claimed result",
                    responsibility_id=responsibility_id, execution_fence=fence,
                )

        self.assertEqual(len(self.slack.posts), 1)
        self.assertEqual(self._outbox_state(job), "sent")
        # Dispatcher recovery may use the original still-valid fence, but it
        # must settle the work before considering the source delivered.
        self.assertTrue(self.dispatcher._delivered(job))
        with ResponsibilityStore(self.project / "state" / "inbox.sqlite3") as store:
            self.assertEqual(store.get(responsibility_id).state, "completed")
        self.assertEqual(len(self.slack.posts), 1)
        self.assertIn(job["revision"], self.inbox.get_receipts(job["message_id"]).completed_revisions)

    def test_bound_responsibility_authority_cannot_downgrade_to_plain_source_reply(self) -> None:
        job, authority = self._active_authority()
        responsibility_id, fence = self._linked_responsibility(job)
        with patch("director.dispatcher.ResponsibilityStore.complete", side_effect=RuntimeError("crash after sent")):
            with self.assertRaisesRegex(RuntimeError, "after sent"):
                self.dispatcher.publish_agent_reply(
                    authority, "do not bypass this claim",
                    responsibility_id=responsibility_id, execution_fence=fence,
                )
        with ResponsibilityStore(self.project / "state" / "inbox.sqlite3") as store:
            store.update(responsibility_id, state="waiting_input")

        downgraded = self.dispatcher.publish_agent_reply(authority, "do not bypass this claim")

        self.assertFalse(downgraded["published"])
        self.assertEqual(len(self.slack.posts), 1)
        self.assertNotIn(job["revision"], self.inbox.get_receipts(job["message_id"]).completed_revisions)
        with ResponsibilityStore(self.project / "state" / "inbox.sqlite3") as store:
            self.assertEqual(store.get(responsibility_id).state, "waiting_input")

    def test_prepared_context_is_bounded_and_speaker_labelled(self) -> None:
        message = self._source()
        self.dispatcher.enqueue(100)
        job = self.dispatcher.db.execute("SELECT * FROM jobs WHERE message_id=?", (message.id,)).fetchone()
        assert job is not None
        oversized = "x" * 6500
        thread = [
            {"ts": ROOT, "user": "U-owner", "text": "root"},
            {"ts": SOURCE, "thread_ts": ROOT, "user": "U-owner", "text": oversized},
        ] + [
            {"ts": f"17100000{i:02d}.000100", "thread_ts": ROOT,
             "user": f"U-{i}", "text": f"context-{i}-" + ("y" * 1700)}
            for i in range(15)
        ]
        self.slack.reply_messages = thread

        context = self.dispatcher._prepare_context(job)

        self.assertEqual(context["source_revision"], job["revision"])
        self.assertTrue(context["truncated"])
        self.assertTrue(context["read"])
        self.assertIn("Verified current source:\n", context["text"])
        self.assertIn("[U-14 at 1710000014.000100]", context["text"])
        self.assertNotIn("context-0-", context["text"])
        self.assertIn("[truncated]", context["text"])
        self.assertIn("Thread context was bounded", context["text"])
        self.assertEqual(self.slack.reply_calls, 1)
        self.assertEqual(self.slack.reactions, [])
