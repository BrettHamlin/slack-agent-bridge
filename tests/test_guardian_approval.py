"""Focused fences for the one-use native Guardian reply approval bridge."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from director.agent_gateway import AgentEvent, SessionBinding
from director.dispatcher import Dispatcher
from director.inbox import InboxStore, InboundPointer
from director.slack_service import SlackService
from director.slack_transport import SlackAllowlist


ROOT = "1789195526.676909"
SOURCE = "1789195527.000100"
ALLOWLIST = SlackAllowlist("T-test", "C-test", "U-owner", "test.slack.com")


class FakeSlack:
    def __init__(self):
        self.posts = []
        self.reactions = []
        self.messages = [
            {"ts": ROOT, "user": "U-owner", "text": "root"},
            {"ts": SOURCE, "thread_ts": ROOT, "user": "U-owner", "text": "reply please"},
        ]

    def conversations_replies(self, **_kwargs):
        return {"ok": True, "channel": "C-test", "messages": list(self.messages)}

    def conversations_history(self, **_kwargs):
        return {"ok": True, "channel": "C-test", "messages": []}

    def reactions_add(self, **kwargs):
        self.reactions.append(kwargs)
        return {"ok": True}

    def chat_postMessage(self, **kwargs):
        self.posts.append(kwargs)
        return {"ok": True, "channel": "C-test", "ts": "1789195528.000100",
                "message": {"ts": "1789195528.000100"}}


class FakeGateway:
    class Driver:
        generation = 7
        alive = True

    def __init__(self, root):
        self.root = root
        self.driver = self.Driver()
        self.approvals = []
        self.prompts = []

    def binding(self, root):
        if root != self.root:
            return None
        return SessionBinding(root, "codex-default", "codex-acp", "session-current")

    def approve_guardian_denied_action(self, binding, review_id, fingerprint):
        self.approvals.append((binding.session_id, review_id, fingerprint))

    def submit(self, binding, text, *, on_turn):
        on_turn("7:2")
        self.prompts.append((binding.session_id, text))
        return "7:2"

    def poll(self):
        return ()

    def close(self, **_kwargs):
        return None


class GuardianApprovalTests(unittest.TestCase):
    def setUp(self):
        self.now = time.time()
        self.temp = tempfile.TemporaryDirectory()
        self.project = Path(self.temp.name)
        (self.project / "state").mkdir()
        self.inbox = InboxStore(self.project / "state" / "inbox.sqlite3")
        self.slack = FakeSlack()
        self.service = SlackService(self.slack, self.inbox, ALLOWLIST, self.project / "state" / "inbox.sqlite3")
        self.dispatcher = Dispatcher(self.project, {
            "database_path": "state/inbox.sqlite3",
            "dispatcher": {"enabled": True, "runtime": "acp", "guardian_approval_ttl_seconds": 2,
                           "acp": {"command": ["synthetic-acp"]}},
        }, self.inbox, self.service)

    def tearDown(self):
        self.dispatcher.close()
        self.service.close()
        self.inbox.close()
        self.temp.cleanup()

    @staticmethod
    def digest(text, *, title=None, emoji=None, preview=None):
        body_data = {"text": text, "responsibility_id": None, "execution_fence": None}
        if any(value is not None for value in (title, emoji, preview)):
            body_data.update(
                conversation_title=title,
                conversation_emoji=emoji,
                conversation_preview=preview,
            )
        body = json.dumps(body_data, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
        return hashlib.sha256(body.encode()).hexdigest()

    def _running_source(self):
        message = self.inbox.ingest(InboundPointer(
            "event-guardian", "T-test", "C-test", SOURCE, SOURCE, ROOT,
        )).message
        self.dispatcher.enqueue(self.now)
        job = self.dispatcher.db.execute("SELECT * FROM jobs WHERE message_id=?", (message.id,)).fetchone()
        with self.dispatcher.db:
            self.dispatcher.db.execute(
                """UPDATE jobs SET state='running',runtime='acp',agent_profile='codex-default',agent_session_id='session-current',
                   agent_generation=7,agent_turn_id='7:1',started_at=? WHERE key=?""",
                (self.now, job["key"]),
            )
        job = self.dispatcher.db.execute("SELECT * FROM jobs WHERE key=?", (job["key"],)).fetchone()
        authority = self.dispatcher._create_publish_authority(job)
        with self.dispatcher.db:
            self.dispatcher.db.execute(
                """UPDATE agent_reply_authorities SET state='active',session_id='session-current',
                   generation=7,turn_id='7:1' WHERE authority=?""", (authority,),
            )
        return self.dispatcher.db.execute("SELECT * FROM jobs WHERE key=?", (job["key"],)).fetchone(), authority

    def _denial(self, job, authority, *, text="approved exact reply", title=None, emoji=None, preview=None,
                digest=None):
        return AgentEvent(
            "progress", job["root"], "session-current", None,
            {"tool": {"raw_output": {"directorApproval": {
                "sessionId": "session-current", "turnId": "native-turn-2ae1", "reviewId": "review-1",
                "fingerprint": "f" * 64, "authority": authority,
                "payloadDigest": self.digest(text, title=title, emoji=emoji, preview=preview) if digest is None else digest,
                "replyDisplay": {"text": text, "conversation_title": title,
                                 "conversation_emoji": emoji, "conversation_preview": preview},
            }}}},
            generation=7,
        )

    def _capture_and_finish(self, job, authority, *, text="approved exact reply", title=None, emoji=None, preview=None):
        event = self._denial(job, authority, text=text, title=title, emoji=emoji, preview=preview)
        self.dispatcher.gateway = type("Events", (), {"poll": lambda _self: (event,), "close": lambda _self, **_k: None})()
        self.dispatcher._consume_acp_events(self.now + 0.1)
        self.dispatcher._finish(job, self.now + 0.2)
        return event

    def test_terminal_pending_projects_exact_adapter_display_only_after_authority_is_blocked(self):
        class Projection:
            configured = True
            def __init__(self): self.calls = []
            def record_guardian_pending(self, **kwargs): self.calls.append(kwargs); return 'feed-token'
        projection = Projection()
        self.dispatcher.feed = projection
        job, authority = self._running_source()
        event = self._denial(job, authority, text='Use “smart quotes” 🚀',
                             title='🚀 Launch plan', emoji='🚀', preview='Ready for your approval.')
        self.dispatcher.gateway = type("Events", (), {"poll": lambda _self: (event,), "close": lambda _self, **_k: None})()
        self.dispatcher._consume_acp_events(self.now + .1)
        self.assertEqual(projection.calls, [], 'active turns cannot expose a clickable approval')
        self.dispatcher._finish(job, self.now + .2)
        self.assertEqual(len(projection.calls), 1)
        call = projection.calls[0]
        self.assertEqual(call['proposal_text'], 'Use “smart quotes” 🚀')
        self.assertEqual((call['title'], call['emoji'], call['preview']), ('🚀 Launch plan', '🚀', 'Ready for your approval.'))
        self.assertNotIn('authority', call)

    def test_actual_native_turn_uuid_with_progress_without_logical_turn_binds_current_authority(self):
        job, authority = self._running_source()
        self._capture_and_finish(job, authority)

        approval = self.dispatcher.db.execute("SELECT * FROM guardian_reply_approvals").fetchone()
        active = self.dispatcher.db.execute("SELECT * FROM agent_reply_authorities WHERE authority=?", (authority,)).fetchone()
        current = self.dispatcher.db.execute("SELECT * FROM jobs WHERE key=?", (job["key"],)).fetchone()
        self.assertEqual((approval["turn_id"], approval["native_turn_id"], approval["state"]),
                         ("7:1", "native-turn-2ae1", "pending"))
        self.assertEqual((active["state"], active["text_digest"]), ("approval_pending", self.digest("approved exact reply")))
        self.assertEqual((current["state"], current["error_code"]), ("blocked", "guardian_approval_pending"))

    def test_denial_immediately_fences_same_turn_and_duplicate_event(self):
        job, authority = self._running_source()
        event = self._denial(job, authority)
        self.dispatcher.gateway = type("Events", (), {"poll": lambda _self: (event,), "close": lambda _self, **_k: None})()
        self.dispatcher._consume_acp_events(self.now + 0.1)
        self.dispatcher._consume_acp_events(self.now + 0.2)

        self.assertEqual(self.dispatcher.db.execute("SELECT count(*) FROM guardian_reply_approvals").fetchone()[0], 1)
        self.assertEqual(self.dispatcher.publish_agent_reply(authority, "approved exact reply"),
                         {"published": False, "state": "authority_rejected"})
        self.assertEqual(self.slack.posts, [])

    def test_pending_lookup_exposes_only_key_root_and_expiry(self):
        job, authority = self._running_source()
        self._capture_and_finish(job, authority)
        pending = self.dispatcher.pending_guardian_approvals()
        self.assertEqual(len(pending), 1)
        self.assertEqual(set(pending[0]), {"key", "root", "expires_at"})
        self.assertEqual((pending[0]["key"], pending[0]["root"]), (job["key"], ROOT))

    def test_explicit_approval_uses_native_handle_once_and_rebinds_same_authority(self):
        job, authority = self._running_source()
        self._capture_and_finish(job, authority)
        gateway = FakeGateway(job["root"])
        self.dispatcher.gateway = gateway

        result = self.dispatcher.approve_guardian_reply(job["key"])
        approval = self.dispatcher.db.execute("SELECT * FROM guardian_reply_approvals").fetchone()
        rebound = self.dispatcher.db.execute("SELECT * FROM agent_reply_authorities WHERE authority=?", (authority,)).fetchone()
        current = self.dispatcher.db.execute("SELECT * FROM jobs WHERE key=?", (job["key"],)).fetchone()
        self.assertEqual(result, {"key": job["key"], "outcome": "guardian_approval_submitted"})
        self.assertEqual(gateway.approvals, [("session-current", "review-1", "f" * 64)])
        self.assertEqual(len(gateway.prompts), 1)
        self.assertIn("exactly the same arguments as that denied", gateway.prompts[0][1])
        self.assertEqual((approval["state"], rebound["authority"], rebound["state"], rebound["turn_id"]),
                         ("used", authority, "active", "7:2"))
        self.assertEqual((current["state"], current["agent_turn_id"]), ("running", "7:2"))
        self.assertEqual(self.dispatcher.approve_guardian_reply(job["key"])["outcome"], "guardian_approval_not_pending")

    def test_approved_retry_delivers_exact_full_feed_payload_once(self):
        text = "The exact approved answer is ready."
        title, emoji, preview = "🧪 Guardian recovery", "🧪", "The approved reply was sent once after local review."
        job, authority = self._running_source()
        self._capture_and_finish(job, authority, text=text, title=title, emoji=emoji, preview=preview)
        self.dispatcher.gateway = FakeGateway(job["root"])
        self.assertEqual(self.dispatcher.approve_guardian_reply(job["key"])["outcome"], "guardian_approval_submitted")

        before = len(self.slack.posts)
        delivered = self.dispatcher.publish_agent_reply(
            authority, text, conversation_title=title, conversation_emoji=emoji, conversation_preview=preview,
        )
        altered = self.dispatcher.publish_agent_reply(
            authority, text + " altered", conversation_title=title, conversation_emoji=emoji,
            conversation_preview=preview,
        )
        duplicate = self.dispatcher.publish_agent_reply(
            authority, text, conversation_title=title, conversation_emoji=emoji, conversation_preview=preview,
        )
        row = self.dispatcher.db.execute("SELECT state,text_digest FROM agent_reply_authorities WHERE authority=?", (authority,)).fetchone()
        self.assertEqual(delivered, {"published": True, "state": "sent"})
        self.assertEqual(altered, {"published": False, "state": "payload_mismatch"})
        self.assertEqual(duplicate, {"published": True, "state": "sent"})
        self.assertEqual(len(self.slack.posts), before + 1)
        self.assertEqual((row["state"], row["text_digest"]),
                         ("published", self.digest(text, title=title, emoji=emoji, preview=preview)))

    def test_missing_approval_metadata_restores_pending_feed_identity_after_exact_delivery(self):
        class Projection:
            configured = True
            def __init__(self): self.pending = []; self.replies = []
            def record_guardian_pending(self, **kwargs): self.pending.append(kwargs); return 'feed-token'
            def record_reply(self, **kwargs): self.replies.append(kwargs); return True
        projection = Projection()
        self.dispatcher.feed = projection
        job, authority = self._running_source()
        text = 'Exact reply without optional metadata 🚀'
        self._capture_and_finish(job, authority, text=text)
        self.assertEqual(projection.pending[0]['title'], 'Pending reply review')
        self.dispatcher.gateway = FakeGateway(job['root'])
        self.assertEqual(self.dispatcher.approve_guardian_reply(job['key'])['outcome'], 'guardian_approval_submitted')
        self.assertEqual(self.dispatcher.publish_agent_reply(authority, text), {'published': True, 'state': 'sent'})
        self.assertEqual(len(projection.replies), 1)
        reply = projection.replies[0]
        self.assertEqual((reply['title'], reply['emoji'], reply['preview']),
                         ('Pending reply review', '🔐', text))

    def test_replayed_native_uuid_for_an_old_authority_cannot_bind_new_turn(self):
        job, old_authority = self._running_source()
        with self.dispatcher.db:
            self.dispatcher.db.execute("UPDATE agent_reply_authorities SET state='closed' WHERE authority=?", (old_authority,))
        new_authority = self.dispatcher._create_publish_authority(job)
        with self.dispatcher.db:
            self.dispatcher.db.execute(
                """UPDATE agent_reply_authorities SET state='active',session_id='session-current',generation=7,turn_id='7:2'
                   WHERE authority=?""", (new_authority,)
            )
            self.dispatcher.db.execute("UPDATE jobs SET agent_turn_id='7:2' WHERE key=?", (job["key"],))
        replay = self._denial(job, old_authority)
        self.dispatcher.gateway = type("Events", (), {"poll": lambda _self: (replay,), "close": lambda _self, **_k: None})()
        self.dispatcher._consume_acp_events(self.now + 0.1)
        self.assertEqual(self.dispatcher.db.execute("SELECT count(*) FROM guardian_reply_approvals").fetchone()[0], 0)
        self.assertEqual(self.dispatcher.db.execute(
            "SELECT state FROM agent_reply_authorities WHERE authority=?", (new_authority,)
        ).fetchone()[0], "active")

    def test_receiver_restart_makes_the_adapter_owned_native_handle_stale(self):
        job, authority = self._running_source()
        self._capture_and_finish(job, authority)
        self.dispatcher.close()
        class Projection:
            configured = True
            def __init__(self): self.states = []
            def set_guardian_approval_state(self, key, state): self.states.append((key, state)); return True
        projection = Projection()
        self.dispatcher = Dispatcher(self.project, {
            "database_path": "state/inbox.sqlite3",
            "dispatcher": {"enabled": True, "runtime": "acp", "guardian_approval_ttl_seconds": 2,
                           "acp": {"command": ["synthetic-acp"]}},
        }, self.inbox, self.service, feed=projection)
        approval = self.dispatcher.db.execute("SELECT state FROM guardian_reply_approvals WHERE job_key=?", (job["key"],)).fetchone()
        current = self.dispatcher.db.execute("SELECT state,error_code FROM jobs WHERE key=?", (job["key"],)).fetchone()
        closed = self.dispatcher.db.execute("SELECT state FROM agent_reply_authorities WHERE authority=?", (authority,)).fetchone()
        self.assertEqual(approval["state"], "stale")
        self.assertEqual(tuple(current), ("blocked", "guardian_approval_adapter_restart"))
        self.assertEqual(closed["state"], "closed")
        self.assertEqual(projection.states, [(job["key"], "restart_inactive")])

    def test_one_approved_retry_cannot_fall_back_to_generic_automatic_retry(self):
        job, authority = self._running_source()
        self._capture_and_finish(job, authority)
        self.dispatcher.gateway = FakeGateway(job["root"])
        self.dispatcher.approve_guardian_reply(job["key"])
        running = self.dispatcher.db.execute("SELECT * FROM jobs WHERE key=?", (job["key"],)).fetchone()
        self.dispatcher._finish(running, self.now + 0.4)

        current = self.dispatcher.db.execute("SELECT * FROM jobs WHERE key=?", (job["key"],)).fetchone()
        self.assertEqual((current["state"], current["error_code"]), ("blocked", "guardian_approved_retry_unpublished"))
        self.dispatcher.acp_locks.pop(job["key"]).close()
        with self.dispatcher.db:
            self.dispatcher.db.execute("UPDATE jobs SET agent_group_id=71 WHERE key=?", (job["key"],))
        with patch.object(Dispatcher, '_runtime_group_gone', return_value=False):
            self.assertEqual(self.dispatcher.reconcile_job(job["key"], retry_if_stopped=False), {
                "key": job["key"], "outcome": "guardian_approval_used",
                "runtime_group_gone": False, "turn_ended": False,
            })
        self.assertEqual(self.dispatcher.db.execute(
            "SELECT state FROM jobs WHERE key=?", (job["key"],)
        ).fetchone()[0], "blocked")

        with patch.object(Dispatcher, '_runtime_group_gone', return_value=True):
            self.assertEqual(self.dispatcher.reconcile_job(job["key"]), {
                "key": job["key"], "outcome": "guardian_approval_used",
                "runtime_group_gone": True, "turn_ended": False,
            })
            self.assertEqual(self.dispatcher.reconcile_job(
                job["key"], retry_if_stopped=True, operator_requested=True,
            ), {
                "key": job["key"], "outcome": "guardian_approval_spent_settled",
                "runtime_group_gone": True, "turn_ended": False,
            })
        settled = self.dispatcher.db.execute("SELECT * FROM jobs WHERE key=?", (job["key"],)).fetchone()
        approval = self.dispatcher.db.execute("SELECT state FROM guardian_reply_approvals WHERE job_key=?", (job["key"],)).fetchone()
        closed = self.dispatcher.db.execute(
            "SELECT state FROM agent_reply_authorities WHERE authority=?", (authority,)
        ).fetchone()
        self.assertEqual((settled["state"], settled["error_code"], approval["state"], closed["state"]),
                         ("failed", "guardian_approved_retry_unpublished", "used", "closed"))
        self.assertEqual(self.dispatcher.reconcile_job(job["key"], retry_if_stopped=True)["outcome"], "not_blocked")
        self.assertEqual(self.dispatcher.approve_guardian_reply(job["key"])["outcome"], "guardian_approval_not_pending")

        fresh = self.inbox.ingest(InboundPointer(
            "event-guardian-fresh", "T-test", "C-test", "1789195527.000300", "1789195527.000300", ROOT,
        )).message
        self.dispatcher.enqueue(self.now + 0.5)
        fresh_job = self.dispatcher.db.execute(
            "SELECT * FROM jobs WHERE message_id=?", (fresh.id,)
        ).fetchone()
        fresh_authority = self.dispatcher._create_publish_authority(fresh_job)
        fresh_binding = self.dispatcher.db.execute(
            "SELECT state FROM agent_reply_authorities WHERE authority=?", (fresh_authority,)
        ).fetchone()
        self.assertEqual((fresh_job["state"], fresh_binding["state"]), ("pending", "preparing"))
        root_lock = self.dispatcher._lock(ROOT)
        self.assertIsNotNone(root_lock)
        root_lock.close()

    def test_spent_approval_keeps_late_verified_delivery_done(self):
        job, _authority = self._running_source()
        self._capture_and_finish(job, _authority)
        self.dispatcher.gateway = FakeGateway(job["root"])
        self.dispatcher.approve_guardian_reply(job["key"])
        running = self.dispatcher.db.execute("SELECT * FROM jobs WHERE key=?", (job["key"],)).fetchone()
        self.dispatcher._finish(running, self.now + 0.4)
        self.dispatcher.acp_locks.pop(job["key"]).close()
        with self.dispatcher.db:
            self.dispatcher.db.execute("UPDATE jobs SET agent_group_id=71 WHERE key=?", (job["key"],))

        self.service.send_outgoing(
            "late verified reply", idempotency_key="dispatch-answer:" + job["key"], thread_ts=ROOT,
        )
        self.assertTrue(self.inbox.mark_completed_if_revision(job["message_id"], job["revision"]))
        with patch.object(Dispatcher, '_runtime_group_gone', return_value=True):
            self.assertEqual(self.dispatcher.reconcile_job(job["key"]), {
                "key": job["key"], "outcome": "done",
                "runtime_group_gone": True, "turn_ended": False,
            })
        settled = self.dispatcher.db.execute("SELECT state,error_code FROM jobs WHERE key=?", (job["key"],)).fetchone()
        self.assertEqual(tuple(settled), ("done", None))

    def test_source_change_or_expiry_closes_pending_authority_without_native_call(self):
        job, authority = self._running_source()
        self._capture_and_finish(job, authority)
        self.inbox.ingest(InboundPointer("event-guardian-edit", "T-test", "C-test", SOURCE,
                                         "1789195527.000200", ROOT, "message_changed"))
        gateway = FakeGateway(job["root"])
        self.dispatcher.gateway = gateway
        self.assertEqual(self.dispatcher.approve_guardian_reply(job["key"])["outcome"], "guardian_approval_source_stale")
        self.assertEqual(gateway.approvals, [])
        self.assertEqual(self.dispatcher.db.execute("SELECT state FROM jobs WHERE key=?", (job["key"],)).fetchone()[0],
                         "superseded")
        self.dispatcher.enqueue(self.now + 0.3)
        self.assertEqual(self.dispatcher.db.execute(
            "SELECT state FROM jobs WHERE message_id=? AND revision=2", (job["message_id"],)
        ).fetchone()[0], "pending")

        # A separate pending record expires to a failed terminal job, releasing
        # the root for a genuinely newer source revision rather than retrying.
        self.tearDown(); self.setUp()
        job, authority = self._running_source()
        self._capture_and_finish(job, authority)
        self.dispatcher._expire_guardian_approvals(self.now + 4)
        current = self.dispatcher.db.execute("SELECT * FROM jobs WHERE key=?", (job["key"],)).fetchone()
        closed = self.dispatcher.db.execute("SELECT * FROM agent_reply_authorities WHERE authority=?", (authority,)).fetchone()
        self.assertEqual((current["state"], current["error_code"], closed["state"]),
                         ("failed", "guardian_approval_expired", "closed"))

    def test_invalid_digest_never_creates_a_pending_approval(self):
        job, authority = self._running_source()
        event = self._denial(job, authority, digest="F" * 64)
        self.dispatcher.gateway = type("Events", (), {"poll": lambda _self: (event,), "close": lambda _self, **_k: None})()
        self.dispatcher._consume_acp_events(101)
        self.assertEqual(self.dispatcher.db.execute("SELECT count(*) FROM guardian_reply_approvals").fetchone()[0], 0)
        state = self.dispatcher.db.execute("SELECT state FROM agent_reply_authorities WHERE authority=?", (authority,)).fetchone()[0]
        self.assertEqual(state, "active")


if __name__ == "__main__":
    unittest.main()
