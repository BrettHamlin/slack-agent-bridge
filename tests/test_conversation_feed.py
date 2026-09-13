import tempfile
import threading
import time
import unittest
from http.client import HTTPMessage
from unittest.mock import patch
from pathlib import Path

from director.conversation_feed import ConversationFeed, ConversationFeedError, feed_target


SOURCE = "C-source"
FEED = "C-feed"
CONFIG = {
    "channel_id": SOURCE,
    "workspace_domain": "example.slack.com",
    "owner_user_id": "U-owner",
    "bot_user_id": "U-bot",
    "conversation_feed": {"enabled": True, "channel_id": FEED},
}


class FakeSlack:
    def __init__(self):
        self.messages = []
        self.posts = []
        self.deletes = []
        self.fail_post = False
        self.fail_delete = False
        self.on_post = None
        self.next_ts = 200.0

    def chat_postMessage(self, **kwargs):
        self.posts.append(kwargs)
        if self.fail_post:
            raise RuntimeError("synthetic post uncertainty")
        if self.on_post is not None:
            callback, self.on_post = self.on_post, None
            callback()
        ts = f"{self.next_ts:.6f}"
        self.next_ts += 1
        self.messages.append({"ts": ts, "client_msg_id": kwargs["client_msg_id"], "bot_id": "U-bot"})
        return {"ok": True, "channel": kwargs["channel"], "message": {"ts": ts}}

    def conversations_history(self, **kwargs):
        return {"ok": True, "channel": kwargs["channel"], "messages": list(reversed(self.messages))}

    def chat_delete(self, **kwargs):
        self.deletes.append(kwargs)
        if self.fail_delete:
            raise RuntimeError("synthetic delete uncertainty")
        self.messages = [message for message in self.messages if message["ts"] != kwargs["ts"]]
        return {"ok": True}


class ConversationFeedTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.path = Path(self.directory.name) / "inbox.sqlite3"
        self.slack = FakeSlack()

    def tearDown(self):
        self.directory.cleanup()

    def create(self):
        return ConversationFeed(self.slack, self.path, CONFIG)

    def test_first_card_uses_stable_identity_and_exact_original_permalink(self):
        feed = self.create()
        self.add_outbox("answer-a", "100.100000", "101.000000")
        self.assertTrue(feed.record_reply(
            root="100.100000", title="Login investigation", emoji="🔐",
            preview="Android still needs its redirect fixed.", outgoing_key="answer-a", outgoing_ts="101.000000",
        ))
        self.assertEqual(feed.reconcile(), {"posted": 1, "deleted": 0, "uncertain": 0})
        post = self.slack.posts[0]
        self.assertEqual(post["channel"], FEED)
        section = post["blocks"][0]["text"]["text"]
        self.assertIn("🔐 Login investigation", section)
        self.assertIn("Android still needs", section)
        button = post["blocks"][1]["elements"][0]
        self.assertEqual(button["url"], "https://example.slack.com/archives/C-source/p100100000?thread_ts=100.100000&cid=C-source")
        feed.close()

    def test_newer_answer_replaces_card_after_confirmed_post_then_deletes_old(self):
        feed = self.create()
        self.add_outbox("answer-a", "100.100000", "101.000000")
        self.add_outbox("answer-b", "100.100000", "102.000000")
        feed.record_reply(root="100.100000", title="Login investigation", emoji="🔐", preview="First answer.", outgoing_key="answer-a", outgoing_ts="101.000000")
        feed.reconcile()
        first = self.slack.posts[-1]
        feed.record_reply(root="100.100000", title="Renamed by accident", emoji="🚀", preview="Second answer.", outgoing_key="answer-b", outgoing_ts="102.000000")
        feed.reconcile()
        self.assertIn("🔐 Login investigation", self.slack.posts[-1]["blocks"][0]["text"]["text"])
        self.assertIn("Second answer.", self.slack.posts[-1]["blocks"][0]["text"]["text"])
        self.assertEqual(self.slack.deletes, [])
        feed.reconcile()
        self.assertEqual(self.slack.deletes[0]["ts"], "200.000000")
        self.assertEqual(len(self.slack.messages), 1)
        feed.close()

    def test_uncertain_post_is_reconciled_without_a_blind_second_post_after_restart(self):
        self.add_outbox("answer-a", "100.100000", "101.000000")
        feed = self.create()
        feed.record_reply(root="100.100000", title="Login investigation", emoji="🔐", preview="Answer.", outgoing_key="answer-a", outgoing_ts="101.000000")
        self.slack.fail_post = True
        self.assertEqual(feed.reconcile()["uncertain"], 1)
        self.assertEqual(len(self.slack.posts), 1)
        feed.close()
        self.slack.fail_post = False
        reopened = self.create()
        self.assertEqual(reopened.reconcile()["uncertain"], 1)
        self.assertEqual(len(self.slack.posts), 1)
        reopened.close()

    def test_accepted_then_timeout_reconciles_after_restart_from_real_shaped_paginated_history(self):
        self.add_outbox("answer-a", "100.100000", "101.000000")
        feed = self.create()
        feed.record_reply(root="100.100000", title="Login", emoji="🔐", preview="Answer.", outgoing_key="answer-a", outgoing_ts="101.000000")
        self.slack.fail_post = True
        feed.reconcile()
        # Slack accepted before the client timed out. Real history has no
        # top-level channel field, and the matching card is on a later page.
        client = feed._connection.execute("SELECT inflight_client_msg_id FROM conversation_feed_sessions").fetchone()[0]
        self.slack.messages = [{"ts": "199.000000", "user": "U-bot", "bot_id": "B-bot"}, {"ts": "200.000000", "client_msg_id": client, "user": "U-bot", "bot_id": "B-bot"}]
        def history(**kwargs):
            if not kwargs.get("cursor"):
                return {"ok": True, "messages": [self.slack.messages[0]], "response_metadata": {"next_cursor": "next"}}
            return {"ok": True, "messages": [self.slack.messages[1]], "response_metadata": {"next_cursor": ""}}
        self.slack.conversations_history = history
        feed.close(); reopened = self.create()
        self.assertEqual(reopened.reconcile()["posted"], 1)
        reopened.close()

    def test_hide_during_uncertain_post_reconciles_and_deletes_the_late_card(self):
        self.add_outbox("answer-a", "100.100000", "101.000000"); self.add_outbox("answer-b", "100.100000", "102.000000")
        feed = self.create(); feed.record_reply(root="100.100000", title="Login", emoji="🔐", preview="Answer.", outgoing_key="answer-a", outgoing_ts="101.000000")
        self.slack.fail_post = True; feed.reconcile(); feed.hide("100.100000")
        client = feed._connection.execute("SELECT inflight_client_msg_id FROM conversation_feed_sessions").fetchone()[0]
        self.slack.fail_post = False; self.slack.messages = [{"ts": "200.000000", "client_msg_id": client, "user": "U-bot", "bot_id": "B-bot"}]
        feed.reconcile(); feed.reconcile()
        self.assertEqual(self.slack.messages, [])
        feed.record_reply(root="100.100000", title="Ignored", emoji="🚀", preview="New answer.", outgoing_key="answer-b", outgoing_ts="102.000000")
        feed.reconcile()
        self.assertIn("New answer.", self.slack.posts[-1]["blocks"][0]["text"]["text"])
        feed.close()

    def test_malformed_history_never_proves_delete_absence(self):
        feed = self.create()
        self.add_outbox("answer-a", "100.100000", "101.000000")
        feed.record_reply(root="100.100000", title="Login", emoji="🔐", preview="Answer.", outgoing_key="answer-a", outgoing_ts="101.000000")
        feed.reconcile(); feed.hide("100.100000")
        self.slack.fail_delete = True
        self.slack.conversations_history = lambda **_: {"ok": True, "messages": "malformed"}
        self.assertEqual(feed.reconcile()["uncertain"], 1)
        self.assertEqual(feed._connection.execute("SELECT state FROM conversation_feed_deletions").fetchone()[0], "uncertain")
        # The malformed evidence remains unknown. It may be checked again,
        # but must never trigger another irreversible delete attempt.
        self.assertEqual(feed.reconcile()["uncertain"], 1)
        self.assertEqual(len(self.slack.deletes), 1)
        feed.close()

    def test_delete_retry_after_is_persisted_and_does_not_block_a_later_session(self):
        class LimitedResponse:
            data = {"ok": False, "error": "ratelimited"}
            headers = HTTPMessage()

        LimitedResponse.headers["retry-after"] = "120"

        feed = self.create()
        self.add_outbox("a", "100.100000", "101.000000")
        self.add_outbox("b", "200.100000", "102.000000")
        feed.record_reply(root="100.100000", title="A", emoji="🔐", preview="A.", outgoing_key="a", outgoing_ts="101.000000")
        feed.reconcile(); feed.hide("100.100000")
        self.slack.chat_delete = lambda **kwargs: (self.slack.deletes.append(kwargs) or LimitedResponse())
        before = time.time()
        self.assertEqual(feed.reconcile()["uncertain"], 1)
        retry_at = feed._connection.execute("SELECT retry_at FROM conversation_feed_deletions").fetchone()[0]
        self.assertGreaterEqual(retry_at, before + 119)
        # The deferred deletion is skipped; an unrelated latest answer can
        # still reach its own card without waiting 120 seconds.
        feed.record_reply(root="200.100000", title="B", emoji="🚀", preview="B.", outgoing_key="b", outgoing_ts="102.000000")
        self.assertEqual(feed.reconcile()["posted"], 1)
        self.assertEqual(len(self.slack.deletes), 1)
        feed.close()

    def test_accepted_delete_timeout_recovers_after_restart_when_later_history_proves_absence(self):
        feed = self.create()
        self.add_outbox("a", "100.100000", "101.000000")
        feed.record_reply(root="100.100000", title="A", emoji="🔐", preview="A.", outgoing_key="a", outgoing_ts="101.000000")
        feed.reconcile(); feed.hide("100.100000")

        def accepted_then_timeout(**kwargs):
            self.slack.deletes.append(kwargs)
            self.slack.messages = [message for message in self.slack.messages if message["ts"] != kwargs["ts"]]
            raise RuntimeError("response lost after Slack accepted deletion")

        self.slack.chat_delete = accepted_then_timeout
        self.slack.conversations_history = lambda **_: (_ for _ in ()).throw(RuntimeError("history unavailable"))
        self.assertEqual(feed.reconcile()["uncertain"], 1)
        self.assertEqual(feed._connection.execute("SELECT state FROM conversation_feed_deletions").fetchone()[0], "uncertain")
        feed.close()

        reopened = self.create()
        # A valid targeted history response later proves the card is gone.
        self.slack.conversations_history = lambda **_: {"ok": True, "messages": []}
        self.assertEqual(reopened.reconcile()["deleted"], 1)
        self.assertEqual(reopened._connection.execute("SELECT state FROM conversation_feed_deletions").fetchone()[0], "deleted")
        self.assertIsNone(reopened._connection.execute("SELECT current_card_ts FROM conversation_feed_sessions").fetchone()[0])
        reopened.close()

    def test_record_reply_is_not_blocked_while_uncertain_delete_reads_history(self):
        feed = self.create()
        self.add_outbox("a", "100.100000", "101.000000")
        self.add_outbox("b", "200.100000", "102.000000")
        feed.record_reply(root="100.100000", title="A", emoji="🔐", preview="A.", outgoing_key="a", outgoing_ts="101.000000")
        feed.reconcile(); feed.hide("100.100000")
        self.slack.fail_delete = True
        feed.reconcile()  # prepared delete -> uncertain with the card still present

        entered, release, recorded = threading.Event(), threading.Event(), threading.Event()
        failures = []

        def blocking_history(**kwargs):
            entered.set()
            if not release.wait(2):
                raise RuntimeError("test release timed out")
            return {"ok": True, "messages": list(self.slack.messages)}

        self.slack.conversations_history = blocking_history
        reconcile_thread = threading.Thread(target=lambda: feed.reconcile(), daemon=True)
        reconcile_thread.start()
        self.assertTrue(entered.wait(1))

        def queue_new_reply():
            try:
                feed.record_reply(root="200.100000", title="B", emoji="🚀", preview="B.", outgoing_key="b", outgoing_ts="102.000000")
                recorded.set()
            except Exception as error:  # pragma: no cover - assertion below reports it
                failures.append(error)

        record_thread = threading.Thread(target=queue_new_reply, daemon=True)
        record_thread.start()
        self.assertTrue(recorded.wait(.3), "history I/O held the durable feed lock")
        release.set()
        reconcile_thread.join(1); record_thread.join(1)
        self.assertFalse(failures)
        self.assertFalse(reconcile_thread.is_alive())
        feed.close()

    def test_palette_exhaustion_still_allocates_unique_marker(self):
        feed = self.create()
        for index in range(25):
            root, ts, key = f"{100 + index}.100000", f"{101 + index}.000000", f"k{index}"
            self.add_outbox(key, root, ts)
            feed.record_reply(root=root, title=f"Session {index}", emoji="🔐", preview="Answer.", outgoing_key=key, outgoing_ts=ts)
        emojis = [row[0] for row in feed._connection.execute("SELECT emoji FROM conversation_feed_sessions").fetchall()]
        self.assertEqual(len(emojis), len(set(emojis)))
        feed.close()

    def test_uncertain_delete_does_not_starve_a_new_session_post(self):
        feed = self.create()
        self.add_outbox("a", "100.100000", "101.000000"); self.add_outbox("b", "200.100000", "102.000000")
        feed.record_reply(root="100.100000", title="A", emoji="🔐", preview="A.", outgoing_key="a", outgoing_ts="101.000000")
        feed.reconcile(); feed.hide("100.100000")
        self.slack.fail_delete = True; feed.reconcile()  # prepared -> uncertain
        feed.record_reply(root="200.100000", title="B", emoji="🚀", preview="B.", outgoing_key="b", outgoing_ts="102.000000")
        self.assertEqual(feed.reconcile()["posted"], 1)
        self.assertIn("B.", self.slack.posts[-1]["blocks"][0]["text"]["text"])
        feed.close()

    def test_hide_rolls_back_if_its_durable_delete_intent_cannot_be_written(self):
        feed = self.create(); self.add_outbox("a", "100.100000", "101.000000")
        feed.record_reply(root="100.100000", title="A", emoji="🔐", preview="A.", outgoing_key="a", outgoing_ts="101.000000"); feed.reconcile()
        with patch.object(feed, "_queue_delete", side_effect=RuntimeError("synthetic")):
            with self.assertRaises(RuntimeError): feed.hide("100.100000")
        self.assertEqual(feed._connection.execute("SELECT visibility FROM conversation_feed_sessions").fetchone()[0], "visible")
        feed.close()

    def test_retry_after_is_persisted_and_a_later_session_can_progress(self):
        class LimitedSlack(FakeSlack):
            def chat_postMessage(self, **kwargs):
                self.posts.append(kwargs)
                return {"ok": False, "error": "ratelimited"}
        self.slack = LimitedSlack(); feed = self.create(); self.add_outbox("a", "100.100000", "101.000000")
        feed.record_reply(root="100.100000", title="A", emoji="🔐", preview="A.", outgoing_key="a", outgoing_ts="101.000000")
        before = __import__('time').time(); feed.reconcile()
        retry_at = feed._connection.execute("SELECT retry_at FROM conversation_feed_sessions").fetchone()[0]
        self.assertGreaterEqual(retry_at, before + .9)
        feed.close()

    def test_permanent_slack_rejection_fences_the_generation_until_a_newer_reply(self):
        feed = self.create()
        self.add_outbox("a", "100.100000", "101.000000")
        self.add_outbox("b", "100.100000", "102.000000")
        feed.record_reply(root="100.100000", title="Login", emoji="🔐", preview="First answer.", outgoing_key="a", outgoing_ts="101.000000")
        self.slack.chat_postMessage = lambda **kwargs: (
            self.slack.posts.append(kwargs) or {"ok": False, "error": "channel_not_found"}
        )
        self.assertEqual(feed.reconcile()["uncertain"], 1)
        row = feed._connection.execute(
            "SELECT post_state, desired_generation, settled_generation FROM conversation_feed_sessions"
        ).fetchone()
        self.assertEqual(tuple(row), ("rejected", 1, 0))
        # A rejection is certain: after its delay expires, the same generation
        # remains fenced instead of being reposted with a new side effect.
        feed._connection.execute("UPDATE conversation_feed_sessions SET retry_at = 0")
        self.assertEqual(feed.reconcile()["uncertain"], 1)
        self.assertEqual(len(self.slack.posts), 1)

        self.slack.chat_postMessage = FakeSlack.chat_postMessage.__get__(self.slack, FakeSlack)
        feed.record_reply(root="100.100000", title="Ignored rename", emoji="🚀", preview="Latest answer.", outgoing_key="b", outgoing_ts="102.000000")
        self.assertEqual(feed.reconcile()["posted"], 1)
        rendered = self.slack.posts[-1]["blocks"][0]["text"]["text"]
        self.assertIn("🔐 Login", rendered)
        self.assertIn("Latest answer.", rendered)
        feed.close()

    def test_a_b_a_settles_one_card_per_session_with_latest_a_at_channel_end(self):
        feed = self.create()
        for key, root, ts in (("a1", "100.100000", "101.000000"), ("b1", "200.100000", "102.000000"), ("a2", "100.100000", "103.000000")):
            self.add_outbox(key, root, ts)
        feed.record_reply(root="100.100000", title="A", emoji="🔐", preview="A first.", outgoing_key="a1", outgoing_ts="101.000000"); feed.reconcile()
        feed.record_reply(root="200.100000", title="B", emoji="🚀", preview="B answer.", outgoing_key="b1", outgoing_ts="102.000000"); feed.reconcile()
        feed.record_reply(root="100.100000", title="Ignored", emoji="🧪", preview="A latest.", outgoing_key="a2", outgoing_ts="103.000000"); feed.reconcile(); feed.reconcile()
        # Slack chronology is oldest-to-newest: B remains before the replacement A.
        texts = [post["blocks"][0]["text"]["text"] for post in self.slack.posts]
        self.assertIn("A latest.", texts[-1]); self.assertEqual(len(self.slack.messages), 2)
        self.assertEqual(self.slack.messages[-1]["ts"], "202.000000")
        feed.close()

    def test_exact_duplicate_confirmed_delivery_never_creates_a_second_card_generation(self):
        feed = self.create(); self.add_outbox("a", "100.100000", "101.000000")
        self.assertTrue(feed.record_reply(root="100.100000", title="A", emoji="🔐", preview="Answer.", outgoing_key="a", outgoing_ts="101.000000"))
        feed.reconcile()
        self.assertFalse(feed.record_reply(root="100.100000", title="Changed", emoji="🚀", preview="Duplicate.", outgoing_key="a", outgoing_ts="101.000000"))
        feed.reconcile()
        self.assertEqual(len(self.slack.posts), 1); self.assertEqual(len(self.slack.messages), 1)
        feed.close()

    def test_pending_delete_is_fenced_to_its_original_feed_target(self):
        feed = self.create(); self.add_outbox("a", "100.100000", "101.000000")
        feed.record_reply(root="100.100000", title="A", emoji="🔐", preview="Answer.", outgoing_key="a", outgoing_ts="101.000000"); feed.reconcile(); feed.hide("100.100000")
        changed = {**CONFIG, "conversation_feed": {"enabled": True, "channel_id": "C-other-feed"}}
        retargeted = ConversationFeed(self.slack, self.path, changed)
        self.assertEqual(retargeted.reconcile()["uncertain"], 1)
        self.assertEqual(self.slack.deletes, [])
        retargeted.close(); feed.close()

    def test_new_reply_during_post_keeps_the_original_inflight_generation_then_posts_latest(self):
        feed = self.create()
        self.add_outbox("answer-a", "100.100000", "101.000000")
        self.add_outbox("answer-b", "100.100000", "102.000000")
        feed.record_reply(root="100.100000", title="Login investigation", emoji="🔐", preview="First.", outgoing_key="answer-a", outgoing_ts="101.000000")
        self.slack.on_post = lambda: feed.record_reply(
            root="100.100000", title="Ignored", emoji="🚀", preview="Second.", outgoing_key="answer-b", outgoing_ts="102.000000"
        )
        feed.reconcile()
        row = feed._connection.execute("SELECT desired_generation, settled_generation, post_state FROM conversation_feed_sessions").fetchone()
        self.assertEqual((row["desired_generation"], row["settled_generation"], row["post_state"]), (2, 0, "prepared"))
        feed.reconcile()
        self.assertIn("Second.", self.slack.posts[-1]["blocks"][0]["text"]["text"])
        feed.close()

    def test_hide_deletes_only_feed_card_and_a_new_reply_resurfaces_same_identity(self):
        feed = self.create()
        self.add_outbox("answer-a", "100.100000", "101.000000")
        self.add_outbox("answer-b", "100.100000", "102.000000")
        feed.record_reply(root="100.100000", title="Login investigation", emoji="🔐", preview="First.", outgoing_key="answer-a", outgoing_ts="101.000000")
        feed.reconcile()
        self.assertTrue(feed.hide("100.100000"))
        feed.reconcile()
        self.assertEqual(len(self.slack.messages), 0)
        feed.record_reply(root="100.100000", title="Ignored title", emoji="🚀", preview="New answer.", outgoing_key="answer-b", outgoing_ts="102.000000")
        feed.reconcile()
        self.assertIn("🔐 Login investigation", self.slack.posts[-1]["blocks"][0]["text"]["text"])
        feed.close()

    def test_hidden_retrying_guardian_card_ignores_old_delivery_but_new_reply_resurfaces(self):
        feed = self.create()
        token = feed.record_guardian_pending(
            root="100.100000", job_key="source-approval", title="Approval", emoji="🧪",
            preview="Needs review.", proposal_text="Exact reply", expires_at=time.time() + 60,
        )
        feed.reconcile()
        card_ts = feed._connection.execute(
            "SELECT current_card_ts FROM conversation_feed_sessions"
        ).fetchone()[0]
        self.assertEqual(feed.begin_guardian_approval(token, card_ts), "source-approval")
        feed.reconcile()
        self.assertTrue(feed.hide("100.100000"))
        feed.reconcile()
        feed._connection.execute(
            "UPDATE conversation_feed_sessions SET latest_activity_at=200 WHERE root='100.100000'"
        )
        row = feed._connection.execute(
            "SELECT latest_activity_at,desired_generation FROM conversation_feed_sessions"
        ).fetchone()
        old_ts = f"{row['latest_activity_at'] - 1:.6f}"
        self.assertFalse(feed.record_reply(
            root="100.100000", title="Ignored", emoji="🚀", preview="Old receipt.",
            outgoing_key="approved-old", outgoing_ts=old_ts,
        ))
        still_hidden = feed._connection.execute(
            "SELECT visibility,approval_state,desired_generation FROM conversation_feed_sessions"
        ).fetchone()
        self.assertEqual(tuple(still_hidden), ("hidden", "retrying", row['desired_generation']))
        self.assertFalse(feed.record_reply(
            root="100.100000", title="Ignored", emoji="🚀", preview="Same receipt.",
            outgoing_key="approved-same", outgoing_ts="200.000000",
        ))

        new_ts = f"{row['latest_activity_at'] + 1:.6f}"
        self.assertTrue(feed.record_reply(
            root="100.100000", title="Ignored", emoji="🚀", preview="New reply.",
            outgoing_key="approved-new", outgoing_ts=new_ts,
        ))
        self.assertEqual(tuple(feed._connection.execute(
            "SELECT visibility,approval_state FROM conversation_feed_sessions"
        ).fetchone()), ("visible", None))
        feed.close()

    def test_import_requires_an_exact_confirmed_original_outbox_receipt(self):
        feed = self.create()
        self.add_outbox("answer-a", "100.100000", "101.000000")
        payload = {
            "root": "100.100000", "title": "Login investigation", "emoji": "🔐",
            "preview": "Android still needs its redirect fixed.", "outgoing_key": "answer-a", "outgoing_ts": "101.000000",
        }
        self.assertTrue(feed.import_record(payload))
        payload["outgoing_ts"] = "101.000001"
        with self.assertRaises(ConversationFeedError):
            feed.import_record(payload)
        feed.close()

    def test_emoji_allocator_avoids_recent_active_collision(self):
        feed = self.create()
        self.add_outbox("a", "100.100000", "101.000000")
        self.add_outbox("b", "200.100000", "102.000000")
        feed.record_reply(root="100.100000", title="First", emoji="🔐", preview="A.", outgoing_key="a", outgoing_ts="101.000000")
        feed.record_reply(root="200.100000", title="Second", emoji="🔐", preview="B.", outgoing_key="b", outgoing_ts="102.000000")
        rows = feed._connection.execute("SELECT root, emoji FROM conversation_feed_sessions ORDER BY root").fetchall()
        self.assertEqual(rows[0]["emoji"], "🔐")
        self.assertNotEqual(rows[1]["emoji"], "🔐")
        feed.close()

    def test_model_text_is_escaped_before_block_kit_rendering(self):
        feed = self.create()
        self.add_outbox("answer-a", "100.100000", "101.000000")
        feed.record_reply(root="100.100000", title="Notify <@U> & review", emoji="🔐", preview="See <https://example.test|this> & decide.", outgoing_key="answer-a", outgoing_ts="101.000000")
        feed.reconcile()
        rendered = self.slack.posts[-1]["blocks"][0]["text"]["text"]
        self.assertIn("&lt;@U&gt; &amp; review", rendered)
        self.assertIn("&lt;https://example.test|this&gt; &amp; decide.", rendered)
        feed.close()

    def test_guardian_pending_first_card_shows_exact_plaintext_and_opaque_single_use_token(self):
        feed = self.create()
        token = feed.record_guardian_pending(
            root="100.100000", job_key="source-approval", title="Login investigation", emoji="🔐",
            preview="Android redirect is ready to send.", proposal_text="Use <@U-owner> & keep this exact text.",
            expires_at=time.time() + 60,
        )
        self.assertIsNotNone(token)
        self.assertEqual(feed.reconcile()["posted"], 1)
        blocks = self.slack.posts[-1]["blocks"]
        proposal = [block for block in blocks if block.get("type") == "section" and block.get("text", {}).get("type") == "plain_text"]
        self.assertEqual([block["text"]["text"] for block in proposal], ["Use <@U-owner> & keep this exact text."])
        approve = next(element for block in blocks if block["type"] == "actions" for element in block["elements"]
                       if element["action_id"] == "director_approve_once")
        self.assertEqual(approve["value"], token)
        self.assertNotIn("authority", approve["value"])
        self.assertNotIn("<@U-owner>", blocks[0]["text"]["text"])
        row = feed._connection.execute("SELECT title,emoji,approval_state FROM conversation_feed_sessions").fetchone()
        self.assertEqual(tuple(row), ("Login investigation", "🔐", "pending"))
        feed.close()

    def test_guardian_stale_card_and_terminal_state_cannot_approve(self):
        feed = self.create()
        token = feed.record_guardian_pending(root="100.100000", job_key="source-approval", title="Approval", emoji="🧪",
                                             preview="Needs review.", proposal_text="Exact reply", expires_at=time.time() + 60)
        feed.reconcile()
        self.assertIsNone(feed.begin_guardian_approval(token, "999.000001"))
        self.assertTrue(feed.set_guardian_approval_state("source-approval", "expired"))
        feed.reconcile()
        blocks = self.slack.posts[-1]["blocks"]
        self.assertFalse(any(element["action_id"] == "director_approve_once"
                             for block in blocks if block["type"] == "actions" for element in block["elements"]))
        self.assertIn("expired", blocks[0]["text"]["text"])
        feed.close()

    def test_guardian_post_snapshots_pending_display_before_terminal_transition(self):
        feed = self.create()
        feed.record_guardian_pending(root="100.100000", job_key="source-approval", title="Approval", emoji="🧪",
                                     preview="Needs review.", proposal_text="Exact reply", expires_at=time.time() + 60)
        self.slack.on_post = lambda: feed.set_guardian_approval_state("source-approval", "failed")
        feed.reconcile()
        first = self.slack.posts[-1]["blocks"]
        self.assertTrue(any(element["action_id"] == "director_approve_once"
                            for block in first if block["type"] == "actions" for element in block["elements"]))
        feed.reconcile()
        second = self.slack.posts[-1]["blocks"]
        self.assertFalse(any(element["action_id"] == "director_approve_once"
                             for block in second if block["type"] == "actions" for element in block["elements"]))
        feed.close()

    def test_guardian_reprojection_preserves_terminal_state_and_token(self):
        feed = self.create()
        token = feed.record_guardian_pending(root="100.100000", job_key="source-approval", title="Approval", emoji="🧪",
                                             preview="Needs review.", proposal_text="Exact reply", expires_at=time.time() + 60)
        self.assertTrue(feed.set_guardian_approval_state("source-approval", "failed"))
        self.assertEqual(feed.record_guardian_pending(root="100.100000", job_key="source-approval", title="Changed", emoji="🚀",
                                                       preview="Changed.", proposal_text="Different", expires_at=time.time() + 60), token)
        row = feed._connection.execute("SELECT title,emoji,approval_state,approval_text FROM conversation_feed_sessions").fetchone()
        self.assertEqual(tuple(row), ("Approval", "🧪", "failed", "Exact reply"))
        feed.close()

    def test_guardian_oversize_proposal_has_no_approve_button_or_partial_body(self):
        feed = self.create()
        proposal = "x" * (46 * 3000 + 1)
        feed.record_guardian_pending(root="100.100000", job_key="source-approval", title="Large reply", emoji="📌",
                                     preview="Requires local review.", proposal_text=proposal, expires_at=time.time() + 60)
        feed.reconcile()
        blocks = self.slack.posts[-1]["blocks"]
        self.assertFalse(any(element["action_id"] == "director_approve_once"
                             for block in blocks if block["type"] == "actions" for element in block["elements"]))
        self.assertFalse(any(block.get("text", {}).get("type") == "plain_text" and block["text"]["text"] in proposal
                             for block in blocks))
        self.assertIn("too large", blocks[0]["text"]["text"])
        feed.close()

    def test_guardian_click_is_card_bound_idempotent_and_reply_restores_normal_card(self):
        feed = self.create()
        token = feed.record_guardian_pending(root="100.100000", job_key="source-approval", title="Approval", emoji="🧪",
                                             preview="Needs review.", proposal_text="Exact reply", expires_at=time.time() + 60)
        feed.reconcile()
        card_ts = self.slack.posts[-1]["client_msg_id"]
        actual_card_ts = feed._connection.execute("SELECT current_card_ts FROM conversation_feed_sessions").fetchone()[0]
        self.assertTrue(feed.reserve_guardian_click(token))
        self.assertFalse(feed.reserve_guardian_click(token))
        self.assertEqual(feed.begin_guardian_approval(token, actual_card_ts), "source-approval")
        self.assertIsNone(feed.begin_guardian_approval(token, actual_card_ts))
        self.assertEqual(feed.reconcile()["posted"], 1)
        self.assertFalse(any(element["action_id"] == "director_approve_once"
                             for block in self.slack.posts[-1]["blocks"] if block["type"] == "actions" for element in block["elements"]))
        now_ts = f"{time.time() + 5:.6f}"
        self.add_outbox("approved", "100.100000", now_ts)
        self.assertTrue(feed.record_reply(root="100.100000", title="ignored", emoji="🚀", preview="Sent once.",
                                          outgoing_key="approved", outgoing_ts=now_ts))
        feed.reconcile(); feed.reconcile()
        self.assertIn("Sent once.", self.slack.posts[-1]["blocks"][0]["text"]["text"])
        self.assertIsNone(feed._connection.execute("SELECT approval_state FROM conversation_feed_sessions").fetchone()[0])
        feed.close()

    def test_disabled_or_same_channel_configuration_never_creates_a_feed_target(self):
        self.assertIsNone(feed_target({**CONFIG, "conversation_feed": {"enabled": False}}))
        with self.assertRaises(ValueError):
            feed_target({**CONFIG, "conversation_feed": {"enabled": True, "channel_id": SOURCE}})

    def add_outbox(self, key, root, ts):
        connection = __import__("sqlite3").connect(self.path)
        connection.execute(
            """CREATE TABLE IF NOT EXISTS slack_outbox (
               idempotency_key TEXT PRIMARY KEY, client_msg_id TEXT, channel_id TEXT, thread_ts TEXT,
               text TEXT, state TEXT, slack_ts TEXT, created_at REAL, updated_at REAL)"""
        )
        connection.execute(
            "INSERT INTO slack_outbox VALUES (?, ?, ?, ?, ?, 'sent', ?, 0, 0)",
            (key, key + "-client", SOURCE, root, "confirmed answer", ts),
        )
        connection.commit()
        connection.close()


if __name__ == "__main__":
    unittest.main()
