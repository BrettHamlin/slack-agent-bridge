"""Integration boundaries for the optional separate conversation feed."""
from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
import tempfile
import queue
import unittest
from unittest.mock import MagicMock, patch

from director.conversation_feed import ConversationFeed
from director.dispatcher import Dispatcher
from director.inbox import InboxStore, InboundPointer
from director.receiver import FeedApprovalInteraction, channel_configs, make_feed_listener, routed_listener, listen_channels
from director.slack_transport import SocketModeAck, SlackAllowlist, make_socket_mode_listener


SOURCE = "C-source"
FEED = "C-feed"


def config(*, feed_channel: str = FEED) -> dict[str, object]:
    return {
        "team_id": "T-owner",
        "channel_id": SOURCE,
        "owner_user_id": "U-owner",
        "bot_user_id": "U-bot",
        "slack_app_id": "A-owner",
        "workspace_domain": "example.test",
        "enabled": True,
        "database_path": "state/inbox.sqlite3",
        "dispatcher": {
            "enabled": True,
            "runtime": "acp",
            "acp": {"command": ["synthetic-acp"]},
        },
        "conversation_feed": {"enabled": True, "channel_id": feed_channel},
    }


class ConfirmedReplyService:
    """Small receiver service double that records the one original reply."""

    def __init__(self) -> None:
        self.sent: list[dict[str, object]] = []
        self._lock = __import__("threading").RLock()

    def send_outgoing(self, text, *, idempotency_key, thread_ts, authorize):
        self.assert_authorized(authorize)
        result = SimpleNamespace(state="sent", slack_ts="200.000001")
        self.sent.append({"text": text, "key": idempotency_key, "thread_ts": thread_ts, "result": result})
        return result

    def reconcile_outgoing(self, idempotency_key):
        for sent in self.sent:
            if sent["key"] == idempotency_key:
                return sent["result"]
        return SimpleNamespace(state="uncertain", slack_ts=None)

    @staticmethod
    def assert_authorized(authorize):
        if not authorize():
            raise AssertionError("original source authorization was unexpectedly lost")


class FeedWeb:
    def __init__(self) -> None:
        self.posts: list[dict[str, object]] = []

    def chat_postMessage(self, **kwargs):
        self.posts.append(kwargs)
        return {"ok": True, "channel": kwargs["channel"], "message": {"ts": "300.000001"}}

    def conversations_history(self, **kwargs):
        return {"ok": True, "channel": kwargs["channel"], "messages": []}

    def chat_delete(self, **kwargs):
        return {"ok": True}


class ConversationFeedIntegrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.project = Path(self.temp.name)
        (self.project / "state").mkdir()
        self.path = self.project / "state" / "inbox.sqlite3"
        self.inbox = InboxStore(self.path)

    def tearDown(self) -> None:
        self.inbox.close()
        self.temp.cleanup()

    def _active_source_authority(self, dispatcher: Dispatcher) -> tuple[str, str]:
        message = self.inbox.ingest(
            InboundPointer("E-source", "T-owner", SOURCE, "100.000001", "100.000001")
        ).message
        dispatcher.enqueue(now=1)
        job = dispatcher.db.execute("SELECT * FROM jobs WHERE kind = 'source'").fetchone()
        authority = dispatcher._create_publish_authority(job)
        with dispatcher.db:
            dispatcher.db.execute(
                """UPDATE jobs SET state='running', runtime='acp', agent_session_id='session-1',
                   agent_generation=1, agent_turn_id='turn-1' WHERE key=?""",
                (job["key"],),
            )
            dispatcher.db.execute(
                """UPDATE agent_reply_authorities SET state='active', session_id='session-1',
                   generation=1, turn_id='turn-1' WHERE authority=?""",
                (authority,),
            )
        return authority, job["key"]

    def _dispatcher(self, *, feed: ConversationFeed | None) -> tuple[Dispatcher, ConfirmedReplyService]:
        service = ConfirmedReplyService()
        dispatcher = Dispatcher(self.project, config(), self.inbox, service, feed=feed)
        return dispatcher, service

    def test_failed_optional_feed_identity_keeps_source_route_ack_and_original_reply(self):
        """Feed verification is isolated from the durable source/reply route."""
        primary = config()
        primary["dispatcher"] = {"enabled": False}
        receiver_client = MagicMock()
        receiver_client.socket_mode_request_listeners = []
        stop = MagicMock()
        stop.is_set.side_effect = [False, True]
        loop = MagicMock()
        with patch("director.receiver.threading.Event", return_value=stop), \
             patch("director.receiver.signal.signal"), \
             patch("director.receiver.credentials", return_value={"SLACK_APP_TOKEN": "test", "SLACK_BOT_TOKEN": "test"}), \
             patch("director.receiver.create_socket_mode_client", return_value=receiver_client), \
             patch("director.receiver.verify_identity"), \
             patch("director.receiver.verify_conversation_feed_identity", side_effect=RuntimeError("feed membership lost")), \
             patch("director.receiver.ReceiptAckWorker"), \
             patch("director.receiver.ChannelLoop", return_value=loop) as construct:
            listen_channels(primary, self.project, lambda _: primary, None)

        self.assertEqual(construct.call_args.kwargs["feed_available"], False)
        listener = receiver_client.socket_mode_request_listeners[0]
        delivered, acknowledgements = [], []
        listener_client = SimpleNamespace(send_socket_mode_response=acknowledgements.append)
        # The live receiver's registered route still accepts a source event.
        # The unit-level listener below makes that route behavior observable
        # without making a second Socket Mode connection.
        source_listener = make_socket_mode_listener(
            SimpleNamespace(ingest=delivered.append),
            SlackAllowlist("T-owner", SOURCE, "U-owner"), response_factory=SocketModeAck,
        )
        route = routed_listener({("T-owner", SOURCE): source_listener}, SocketModeAck)
        route(listener_client, SimpleNamespace(
            type="events_api", envelope_id="E-ack", payload={
                "team_id": "T-owner", "event_id": "E-route",
                "event": {"type": "message", "channel": SOURCE, "user": "U-owner", "ts": "101.000001"},
            },
        ))
        self.assertEqual(len(delivered), 1)
        self.assertEqual(acknowledgements, [SocketModeAck("E-ack")])
        # Verify the same outage does not stop a valid original publish.
        unavailable_feed = ConversationFeed(FeedWeb(), self.path, config())
        unavailable_feed.disable()
        dispatcher, service = self._dispatcher(feed=unavailable_feed)
        try:
            authority, _ = self._active_source_authority(dispatcher)
            result = dispatcher.publish_agent_reply(authority, "Original reply.", conversation_title="Reply", conversation_emoji="✉️", conversation_preview="The original conversation was answered.")
            self.assertEqual(result, {"published": True, "state": "sent"})
            self.assertEqual(len(service.sent), 1)
        finally:
            dispatcher.close()
            unavailable_feed.close()

    def test_unavailable_feed_persists_metadata_and_restart_recovers_projection_without_agent(self):
        web = FeedWeb()
        unavailable_feed = ConversationFeed(web, self.path, config())
        unavailable_feed.disable()
        dispatcher, service = self._dispatcher(feed=unavailable_feed)
        try:
            authority, key = self._active_source_authority(dispatcher)
            result = dispatcher.publish_agent_reply(
                authority, "The original answer is delivered.",
                conversation_title="Login investigation", conversation_emoji="🔐",
                conversation_preview="Android still needs its redirect fixed.",
            )
            self.assertEqual(result, {"published": True, "state": "sent"})
            row = unavailable_feed._connection.execute(
                "SELECT title, emoji, latest_preview FROM conversation_feed_sessions"
            ).fetchone()
            self.assertEqual(tuple(row), ("Login investigation", "🔐", "Android still needs its redirect fixed."))
        finally:
            dispatcher.close()
            unavailable_feed.close()

        recovered_feed = ConversationFeed(web, self.path, config())
        recovered_dispatcher = Dispatcher(self.project, config(), self.inbox, service, feed=recovered_feed)
        try:
            self.assertEqual(recovered_dispatcher.recover_conversation_feed(), 1)
            self.assertEqual(recovered_feed.reconcile()["posted"], 1)
            self.assertEqual(len(web.posts), 1)
            self.assertEqual(len(service.sent), 1, "recovery must not invoke a new agent or original Slack send")
            self.assertEqual(service.sent[0]["key"], "dispatch-answer:" + key)
        finally:
            recovered_dispatcher.close()
            recovered_feed.close()

    def test_recovery_does_not_unhide_a_hidden_guardian_delivery_with_stale_display_state(self):
        web = FeedWeb()
        feed = ConversationFeed(web, self.path, config())
        dispatcher, service = self._dispatcher(feed=feed)
        try:
            authority, key = self._active_source_authority(dispatcher)
            self.assertEqual(dispatcher.publish_agent_reply(
                authority, "The approved original answer.",
                conversation_title="Approval", conversation_emoji="🔐", conversation_preview="Approved answer.",
            ), {"published": True, "state": "sent"})
            self.assertEqual(feed.reconcile()["posted"], 1)
            job = dispatcher.db.execute("SELECT root FROM jobs WHERE key=?", (key,)).fetchone()
            with dispatcher.db:
                dispatcher.db.execute(
                    """INSERT INTO guardian_reply_approvals(
                        approval_id,job_key,authority,root,session_id,generation,turn_id,native_turn_id,
                        review_id,fingerprint,payload_digest,state,created_at,expires_at
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    ("approval", key, authority, job["root"], "session-1", 1, "turn-1", "native",
                     "review", "fingerprint", "digest", "used", 1, 2),
                )
            feed._connection.execute(
                "UPDATE conversation_feed_sessions SET approval_state='retrying' WHERE root=?", (job["root"],)
            )
            self.assertTrue(feed.hide(job["root"]))
            feed.reconcile()
        finally:
            dispatcher.close()
            feed.close()

        recovered_feed = ConversationFeed(web, self.path, config())
        recovered_dispatcher = Dispatcher(self.project, config(), self.inbox, service, feed=recovered_feed)
        try:
            self.assertEqual(recovered_dispatcher.recover_conversation_feed(), 1)
            self.assertEqual(recovered_feed.reconcile()["posted"], 0)
            row = recovered_feed._connection.execute(
                "SELECT visibility,approval_state FROM conversation_feed_sessions"
            ).fetchone()
            self.assertEqual(tuple(row), ("hidden", "retrying"))
            self.assertEqual(len(web.posts), 1)
            self.assertEqual(len(service.sent), 1, "recovery must not invoke a new original reply")
        finally:
            recovered_dispatcher.close()
            recovered_feed.close()

    def test_malformed_optional_metadata_reports_projection_error_after_original_publish(self):
        feed = ConversationFeed(FeedWeb(), self.path, config())
        dispatcher, service = self._dispatcher(feed=feed)
        try:
            authority, _ = self._active_source_authority(dispatcher)
            result = dispatcher.publish_agent_reply(
                authority, "Original reply remains valid.",
                conversation_title="bad\ntitle", conversation_emoji="🔐", conversation_preview="Useful preview.",
            )
            self.assertEqual(result, {"published": True, "state": "sent"})
            self.assertEqual(len(service.sent), 1)
            self.assertEqual(dispatcher.feed_error, "ValueError")
            self.assertIsNone(feed._connection.execute("SELECT 1 FROM conversation_feed_sessions").fetchone())
        finally:
            dispatcher.close()
            feed.close()

    def test_feed_events_and_open_url_interactions_are_acknowledged_without_source_or_agent_dispatch(self):
        ingested, source_dispatch, acknowledgements = [], [], []
        source_listener = make_socket_mode_listener(
            SimpleNamespace(ingest=ingested.append), SlackAllowlist("T-owner", SOURCE, "U-owner"),
            response_factory=SocketModeAck,
        )
        route = routed_listener({("T-owner", SOURCE): source_listener}, SocketModeAck)
        client = SimpleNamespace(send_socket_mode_response=acknowledgements.append)
        for request_type, payload in (
            ("events_api", {"team_id": "T-owner", "event_id": "E-feed", "event": {"type": "message", "channel": FEED, "user": "U-owner", "ts": "100.000001"}}),
            ("interactive", {"team": {"id": "T-owner"}, "channel": {"id": FEED}, "actions": [{"action_id": "director_open_conversation", "url": "https://example.test/archives/C-source/p100000001"}]}),
        ):
            route(client, SimpleNamespace(type=request_type, envelope_id=request_type, payload=payload))
        self.assertEqual(ingested, [])
        self.assertEqual(source_dispatch, [])
        self.assertEqual(acknowledgements, [SocketModeAck("events_api"), SocketModeAck("interactive")])

    def test_approval_feed_callback_acks_only_owner_and_queues_opaque_token(self):
        class Target:
            channel_id = FEED
        class Feed:
            target = Target()
            def __init__(self): self.values = []
            def reserve_guardian_click(self, value):
                self.values.append(value); return value == 'a' * 32
        feed, requests, acknowledgements = Feed(), queue.SimpleQueue(), []
        handler = FeedApprovalInteraction(config(), feed, requests)
        listener = make_feed_listener(handler, SocketModeAck)
        client = SimpleNamespace(send_socket_mode_response=acknowledgements.append)
        payload = {
            'team': {'id': 'T-owner'}, 'channel': {'id': FEED}, 'user': {'id': 'U-owner'},
            'container': {'message_ts': '300.000001'},
            'actions': [{'action_id': 'director_approve_once', 'value': 'a' * 32}],
        }
        listener(client, SimpleNamespace(type='interactive', envelope_id='E-approved', payload=payload))
        self.assertEqual(requests.get_nowait(), ('a' * 32, '300.000001'))
        self.assertEqual(acknowledgements, [SocketModeAck('E-approved')])
        # Wrong owner and ordinary feed events are acknowledged but never fed
        # into source intake nor the approval queue.
        payload['user'] = {'id': 'U-other'}
        listener(client, SimpleNamespace(type='interactive', envelope_id='E-wrong', payload=payload))
        payload['user'] = {'id': 'U-owner'}; payload['team'] = {'id': 'T-other'}
        listener(client, SimpleNamespace(type='interactive', envelope_id='E-team', payload=payload))
        payload['team'] = {'id': 'T-owner'}; payload['channel'] = {'id': 'C-other'}
        listener(client, SimpleNamespace(type='interactive', envelope_id='E-channel', payload=payload))
        listener(client, SimpleNamespace(type='events_api', envelope_id='E-event', payload={
            'team_id': 'T-owner', 'event': {'type': 'message', 'channel': FEED, 'ts': '300.000002'},
        }))
        self.assertTrue(requests.empty())
        self.assertEqual(acknowledgements[-4:], [SocketModeAck('E-wrong'), SocketModeAck('E-team'), SocketModeAck('E-channel'), SocketModeAck('E-event')])

    def test_loop_revalidates_queued_card_before_native_approval(self):
        from director.receiver import ChannelLoop
        class Feed:
            def __init__(self): self.begins = []; self.states = []
            def begin_guardian_approval(self, token, ts): self.begins.append((token, ts)); return 'source-1-r1'
            def set_guardian_approval_state(self, key, state): self.states.append((key, state)); return True
        feed = Feed()
        dispatcher = SimpleNamespace(approve_guardian_reply=lambda key: {'key': key, 'outcome': 'guardian_approval_submitted'})
        worker = SimpleNamespace(wake=MagicMock())
        loop = object.__new__(ChannelLoop)
        loop.feed, loop.dispatcher, loop.feed_worker = feed, dispatcher, worker
        loop.feed_approval_requests = queue.SimpleQueue()
        loop.feed_approval_requests.put(('a' * 32, '300.000001'))
        loop._process_feed_approval()
        self.assertEqual(feed.begins, [('a' * 32, '300.000001')])
        self.assertEqual(feed.states, [])
        self.assertEqual(worker.wake.call_count, 1)

    def test_cross_config_source_feed_collision_and_shared_feed_are_rejected(self):
        primary = config(feed_channel="C-primary-feed")
        primary["additional_configs"] = ["config/test.json"]
        test = {
            **config(feed_channel="C-primary-feed"),
            "channel_id": "C-test", "database_path": "state/testing/inbox.sqlite3",
            "environment": "test", "additional_configs": [],
        }
        with self.assertRaisesRegex(ValueError, "distinct"):
            channel_configs(primary, self.project, lambda _: test)
        collision = {**test, "conversation_feed": {"enabled": True, "channel_id": SOURCE}}
        with self.assertRaisesRegex(ValueError, "distinct"):
            channel_configs(primary, self.project, lambda _: collision)


if __name__ == "__main__":
    unittest.main()
