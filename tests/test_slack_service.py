from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import sqlite3
import tempfile
import time
from types import SimpleNamespace
import unittest

from director.inbox import InboxStore, InboundPointer
from director.slack_service import SlackService, SlackServiceError
from director.slack_transport import SlackAllowlist, SlackSourceError, normalize_event


ALLOWLIST = SlackAllowlist("T-allowed", "C-allowed", "U-owner", "director.test")


@dataclass(frozen=True)
class StoredMessage:
    id: int = 7
    source_team_id: str = "T-allowed"
    source_channel_id: str = "C-allowed"
    source_ts: str = "1710000000.000100"
    event_ts: str = "1710000000.000100"
    thread_ts: str | None = None
    event_type: str = "message"
    revision: int = 1


@dataclass(frozen=True)
class SourceThread:
    source_team_id: str = "T-allowed"
    source_channel_id: str = "C-allowed"
    thread_ts: str = "1710000000.000100"


class FakeStore:
    def __init__(self, message: StoredMessage | None = None, threads: tuple[SourceThread, ...] = ()) -> None:
        self.message = message or StoredMessage()
        self.threads = threads
        self.ingested: list[InboundPointer] = []
        self.reads: list[tuple[int, int]] = []
        self.advance_after_get = False
        self._gets = 0

    def get_message(self, message_id: int) -> StoredMessage:
        self._gets += 1
        if self.advance_after_get and self._gets > 1:
            return StoredMessage(revision=2, event_ts="1710000001.000100")
        return self.message

    def mark_read_if_revision(self, message_id: int, revision: int, *, now: float | None = None) -> bool:
        self.reads.append((message_id, revision))
        return revision == self.message.revision

    def ingest(self, pointer: InboundPointer) -> None:
        self.ingested.append(pointer)

    def list_source_threads(self) -> tuple[SourceThread, ...]:
        return self.threads


class UpdatingStore(FakeStore):
    def ingest(self, pointer: InboundPointer) -> object:
        self.ingested.append(pointer)
        self.message = StoredMessage(revision=self.message.revision + 1, event_ts=pointer.event_ts)
        return SimpleNamespace(message=self.message)


class FakeSlack:
    def __init__(self) -> None:
        self.reply_pages: dict[str, list[dict[str, object]]] = {}
        self.history_pages: list[dict[str, object]] = []
        self.reactions: list[dict[str, object]] = []
        self.posts: list[dict[str, object]] = []
        self.updates: list[dict[str, object]] = []
        self.fail_post = False
        self.fail_update = False
        self.already_reacted = False
        self.sdk_post_response = False

    def conversations_replies(self, **kwargs: object) -> dict[str, object]:
        root = str(kwargs["ts"])
        pages = self.reply_pages[root]
        cursor = kwargs.get("cursor")
        index = 0 if cursor is None else int(str(cursor))
        return pages[index]

    def conversations_history(self, **kwargs: object) -> dict[str, object]:
        cursor = kwargs.get("cursor")
        return self.history_pages[0 if cursor is None else int(str(cursor))]

    def reactions_add(self, **kwargs: object) -> dict[str, object]:
        self.reactions.append(kwargs)
        if self.already_reacted:
            raise AlreadyReacted()
        return {"ok": True}

    def chat_postMessage(self, **kwargs: object) -> dict[str, object]:
        self.posts.append(kwargs)
        if self.fail_post:
            raise RuntimeError("connection ended after request write")
        response = {"ok": True, "message": {"ts": "1720000000.000100"}}
        return ResponseData(response) if self.sdk_post_response else response

    def chat_update(self, **kwargs: object) -> dict[str, object]:
        self.updates.append(kwargs)
        if self.fail_update:
            raise RuntimeError("connection ended after request write")
        return {"ok": True, "ts": kwargs["ts"]}


class AlreadyReacted(Exception):
    def __init__(self) -> None:
        self.response = ResponseData({"ok": False, "error": "already_reacted"})


class ResponseData:
    def __init__(self, data: dict[str, object]) -> None:
        self.data = data


class SlackReadTests(unittest.TestCase):
    def test_deleted_source_cannot_receive_read_reaction_or_receipt(self) -> None:
        slack = FakeSlack()
        slack.reply_pages['1710000000.000100'] = [{'ok': True, 'messages': []}]
        store = FakeStore(StoredMessage(event_type='message_deleted', revision=2))
        with tempfile.TemporaryDirectory() as directory:
            with SlackService(slack, store, ALLOWLIST, Path(directory) / 'outbox.sqlite3') as service:
                with self.assertRaises(SlackSourceError):
                    service.mark_read_after_manager_command(7, expected_revision=2)
        self.assertEqual(slack.reactions, [])
        self.assertEqual(store.reads, [])

    def test_manager_read_fetches_reverifies_reacts_then_fences_receipt(self) -> None:
        slack = FakeSlack()
        slack.reply_pages["1710000000.000100"] = [
            {"ok": True, "channel": "C-allowed", "messages": [{"ts": "1710000000.000100", "user": "U-owner"}]}
        ]
        store = FakeStore()
        with tempfile.TemporaryDirectory() as directory:
            with SlackService(slack, store, ALLOWLIST, Path(directory) / "outbox.sqlite3") as service:
                self.assertTrue(service.mark_read_after_manager_command(7))

        self.assertEqual(slack.reactions, [{"channel": "C-allowed", "timestamp": "1710000000.000100", "name": "white_check_mark"}])
        self.assertEqual(store.reads, [(7, 1)])

    def test_reaction_already_present_is_a_success_and_stale_revision_is_not_receipted(self) -> None:
        slack = FakeSlack()
        slack.already_reacted = True
        slack.reply_pages["1710000000.000100"] = [
            {"ok": True, "channel": "C-allowed", "messages": [{"ts": "1710000000.000100", "user": "U-owner"}]}
        ]
        store = FakeStore()
        with tempfile.TemporaryDirectory() as directory:
            with SlackService(slack, store, ALLOWLIST, Path(directory) / "outbox.sqlite3") as service:
                self.assertTrue(service.mark_read_after_manager_command(7))

        self.assertEqual(store.reads, [(7, 1)])

        stale_store = FakeStore()
        stale_store.advance_after_get = True
        with tempfile.TemporaryDirectory() as directory:
            with SlackService(slack, stale_store, ALLOWLIST, Path(directory) / "outbox.sqlite3") as service:
                self.assertFalse(service.mark_read_after_manager_command(7))
        self.assertEqual(stale_store.reads, [])

    def test_source_fetch_ingests_a_newer_edit_and_expected_revision_refuses_old_read(self) -> None:
        slack = FakeSlack()
        slack.reply_pages["1710000000.000100"] = [
            {
                "ok": True,
                "channel": "C-allowed",
                "messages": [
                    {
                        "ts": "1710000000.000100",
                        "user": "U-owner",
                        "edited": {"user": "U-owner", "ts": "1710000001.000100"},
                    }
                ],
            }
        ]
        store = UpdatingStore()
        with tempfile.TemporaryDirectory() as directory:
            with SlackService(slack, store, ALLOWLIST, Path(directory) / "outbox.sqlite3") as service:
                fetched = service.fetch_owner_source_evidence(store.message)

        self.assertTrue(fetched.source_updated)
        self.assertEqual(fetched.message.revision, 2)
        self.assertEqual(store.ingested[0].event_ts, "1710000001.000100")

        stale_store = UpdatingStore()
        with tempfile.TemporaryDirectory() as directory:
            with SlackService(slack, stale_store, ALLOWLIST, Path(directory) / "outbox.sqlite3") as service:
                self.assertFalse(service.mark_read_after_manager_command(7, expected_revision=1))
        self.assertEqual(slack.reactions, [])
        self.assertEqual(stale_store.reads, [])


class SlackOutboxTests(unittest.TestCase):
    def test_outbox_is_durable_and_client_message_id_is_idempotent(self) -> None:
        slack = FakeSlack()
        slack.sdk_post_response = True
        store = FakeStore()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "outbox.sqlite3"
            with SlackService(slack, store, ALLOWLIST, path) as service:
                first = service.send_outgoing("done", idempotency_key="action-1", thread_ts="1710000000.000100")
                repeated = service.send_outgoing("done", idempotency_key="action-1", thread_ts="1710000000.000100")
            with SlackService(slack, store, ALLOWLIST, path) as reopened:
                after_reopen = reopened.reconcile_outgoing("action-1")

        self.assertEqual(first.state, "sent")
        self.assertEqual(first.client_msg_id, repeated.client_msg_id)
        self.assertEqual(after_reopen.state, "sent")
        self.assertEqual(len(slack.posts), 1)
        self.assertEqual(slack.posts[0]["client_msg_id"], first.client_msg_id)

    def test_uncertain_send_is_reconciled_without_automatic_duplicate_retry(self) -> None:
        slack = FakeSlack()
        slack.fail_post = True
        slack.history_pages = [{"ok": True, "channel": "C-allowed", "messages": []}]
        store = FakeStore()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "outbox.sqlite3"
            with SlackService(slack, store, ALLOWLIST, path) as service:
                uncertain = service.send_outgoing("done", idempotency_key="action-2")
            slack.fail_post = False
            with SlackService(slack, store, ALLOWLIST, path) as reopened:
                again = reopened.send_outgoing("done", idempotency_key="action-2")

        self.assertEqual(uncertain.state, "pending_uncertain")
        self.assertEqual(again.state, "pending_uncertain")
        self.assertEqual(len(slack.posts), 1)

    def test_reconciliation_finds_client_message_in_history(self) -> None:
        slack = FakeSlack()
        slack.fail_post = True
        slack.history_pages = [{"ok": True, "channel": "C-allowed", "messages": []}]
        store = FakeStore()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "outbox.sqlite3"
            with SlackService(slack, store, ALLOWLIST, path) as service:
                initial = service.send_outgoing("done", idempotency_key="action-3")
                slack.history_pages = [
                    {
                        "ok": True,
                        "channel": "C-allowed",
                        "messages": [
                            {"ts": "1720000000.000100", "client_msg_id": initial.client_msg_id, "bot_id": "B-director"}
                        ],
                    }
                ]
                recovered = service.reconcile_outgoing("action-3")

        self.assertEqual(recovered.state, "sent")
        self.assertEqual(recovered.slack_ts, "1720000000.000100")

    def test_reconciliation_does_not_accept_a_user_lookalike_client_id(self) -> None:
        slack = FakeSlack()
        slack.fail_post = True
        slack.history_pages = [{"ok": True, "channel": "C-allowed", "messages": []}]
        store = FakeStore()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "outbox.sqlite3"
            with SlackService(slack, store, ALLOWLIST, path) as service:
                initial = service.send_outgoing("done", idempotency_key="action-user-lookalike")
                slack.history_pages = [
                    {
                        "ok": True,
                        "channel": "C-allowed",
                        "messages": [{"ts": "1720000000.000100", "client_msg_id": initial.client_msg_id, "user": "U-owner"}],
                    }
                ]
                result = service.reconcile_outgoing("action-user-lookalike")

        self.assertEqual(result.state, "pending_uncertain")


def card_payload(card_id: str, action_id: str, action_ts: str, *, version: int = 1, defer_seconds: int | None = None):
    action: dict[str, object] = {
        "action_id": action_id,
        "action_ts": action_ts,
        "value": f"{card_id}:{version}",
    }
    if action_id == "director_later":
        action["selected_option"] = {"value": f"{card_id}:{version}:{defer_seconds}"}
    return {
        "type": "block_actions",
        "team": {"id": "T-allowed"},
        "channel": {"id": "C-allowed"},
        "user": {"id": "U-owner"},
        "container": {"message_ts": "1720000000.000100"},
        "actions": [action],
    }


class SlackInboxCardTests(unittest.TestCase):
    def create(self, service: SlackService):
        return service.create_inbox_card(
            "Weekend plans",
            "I have options ready. The next step is choosing whether to make time for this now.",
            "https://director.test/archives/C-allowed/p1710000000000100?thread_ts=1710000000.000100&cid=C-allowed",
            idempotency_key="weekend-plans",
        )

    def test_card_is_durable_and_retries_do_not_duplicate_the_post(self) -> None:
        slack = FakeSlack()
        with tempfile.TemporaryDirectory() as directory:
            with SlackService(slack, FakeStore(), ALLOWLIST, Path(directory) / "inbox.sqlite3") as service:
                first = self.create(service)
                repeated = self.create(service)

        self.assertEqual(first.state, "pending")
        self.assertEqual(first.delivery_state, "sent")
        self.assertEqual(first.id, repeated.id)
        self.assertEqual(len(slack.posts), 1)
        self.assertEqual(slack.posts[0]["channel"], "C-allowed")
        self.assertIn("blocks", slack.posts[0])

    def test_open_is_a_noop_and_later_updates_the_same_card_once(self) -> None:
        slack = FakeSlack()
        started = time.time()
        with tempfile.TemporaryDirectory() as directory:
            with SlackService(slack, FakeStore(), ALLOWLIST, Path(directory) / "inbox.sqlite3") as service:
                card = self.create(service)
                self.assertIsNone(service.handle_interaction(card_payload(card.id, "director_open_conversation", "1710000001.000100")))
                self.assertEqual(service._get_inbox_card(card.id).state, "pending")

                first = service.handle_interaction(
                    card_payload(card.id, "director_later", "1710000002.000100", defer_seconds=86400)
                )
                retry = service.handle_interaction(
                    card_payload(card.id, "director_later", "1710000002.000100", defer_seconds=86400)
                )

                self.assertIsNotNone(first)
                self.assertIsNotNone(retry)
                current = service._get_inbox_card(card.id)
                receipt_count = service._connection.execute(
                    "SELECT COUNT(*) FROM slack_inbox_card_actions WHERE card_id = ?", (card.id,)
                ).fetchone()[0]
                self.assertEqual(service.reconcile_pending_inbox_card_renders(), 1)
                repaired = service.repair_inbox_card_link(
                    "weekend-plans",
                    "https://director.test/archives/C-allowed/p1710000001000100?thread_ts=1710000000.000100&cid=C-allowed",
                )

        self.assertEqual(current.state, "deferred")
        self.assertEqual(current.version, 2)
        self.assertGreaterEqual(current.revisit_at or 0, started + 86400)
        self.assertLess((current.revisit_at or 0) - started, 86402)
        self.assertEqual(receipt_count, 1)
        self.assertEqual(repaired.state, "deferred")
        self.assertEqual(repaired.version, 2)
        self.assertEqual(len(slack.posts), 1)
        self.assertEqual(len(slack.updates), 2)
        self.assertTrue(all(update["ts"] == card.slack_ts for update in slack.updates))
        self.assertTrue(any(element.get("url") == repaired.conversation_url for block in slack.updates[-1]["blocks"] for element in block.get("elements", [])))

    def test_drop_preserves_the_card_and_stale_or_untrusted_actions_cannot_change_it(self) -> None:
        slack = FakeSlack()
        with tempfile.TemporaryDirectory() as directory:
            with SlackService(slack, FakeStore(), ALLOWLIST, Path(directory) / "inbox.sqlite3") as service:
                card = self.create(service)
                untrusted = card_payload(card.id, "director_drop", "1710000003.000100")
                untrusted["user"] = {"id": "U-other"}
                self.assertIsNone(service.handle_interaction(untrusted))
                self.assertEqual(service._get_inbox_card(card.id).state, "pending")

                wrong_message = card_payload(card.id, "director_drop", "1710000003.000200")
                wrong_message["container"] = {"message_ts": "1720000009.000100"}
                self.assertIsNone(service.handle_interaction(wrong_message))
                self.assertEqual(service._get_inbox_card(card.id).state, "pending")

                dropped = service.handle_interaction(card_payload(card.id, "director_drop", "1710000004.000100"))
                self.assertIsNotNone(dropped)
                self.assertEqual(dropped.card.state, "dropped")
                self.assertIsNone(service.handle_interaction(card_payload(card.id, "director_drop", "1710000005.000100")))
                current = service._get_inbox_card(card.id)
                self.assertEqual(service.reconcile_pending_inbox_card_renders(), 1)

        self.assertEqual(current.state, "dropped")
        self.assertEqual(current.version, 2)
        self.assertEqual(len(slack.updates), 1)
        self.assertTrue(any(element.get("url") == card.conversation_url for block in slack.updates[0]["blocks"] for element in block.get("elements", [])))

    def test_receiver_resurfaces_once_after_restart_and_sends_one_top_level_nudge(self) -> None:
        slack = FakeSlack()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "inbox.sqlite3"
            with SlackService(slack, FakeStore(), ALLOWLIST, path) as service:
                card = self.create(service)
                deferred = service.handle_interaction(
                    card_payload(card.id, "director_later", "1710000010.000100", defer_seconds=86400)
                )
                self.assertIsNotNone(deferred)
                due_at = service._get_inbox_card(card.id).revisit_at
                self.assertIsNotNone(due_at)
                self.assertEqual(service.reconcile_pending_inbox_card_renders(), 1)

            with SlackService(slack, FakeStore(), ALLOWLIST, path) as before_due:
                self.assertEqual(before_due.resurface_due_inbox_cards(now=(due_at or 0) - 1), 0)
                self.assertEqual(before_due.deliver_pending_inbox_card_nudges(), 0)

            with SlackService(slack, FakeStore(), ALLOWLIST, path) as after_due:
                self.assertEqual(after_due.resurface_due_inbox_cards(now=(due_at or 0) + 1), 1)
                current = after_due._get_inbox_card(card.id)
                self.assertEqual(current.state, "pending")
                self.assertIsNone(current.revisit_at)
                self.assertEqual(current.version, 3)
                self.assertEqual(after_due.reconcile_pending_inbox_card_renders(), 1)
                self.assertEqual(after_due.deliver_pending_inbox_card_nudges(), 1)

            with SlackService(slack, FakeStore(), ALLOWLIST, path) as retry:
                self.assertEqual(retry.resurface_due_inbox_cards(now=(due_at or 0) + 2), 0)
                self.assertEqual(retry.reconcile_pending_inbox_card_renders(), 0)
                self.assertEqual(retry.deliver_pending_inbox_card_nudges(), 0)

        self.assertEqual(len(slack.posts), 2)
        nudge = slack.posts[-1]
        self.assertNotIn("blocks", nudge)
        self.assertIn("<@U-owner>", str(nudge["text"]))
        self.assertIn("Open the action card", str(nudge["text"]))
        self.assertIn("https://director.test/archives/C-allowed/p1720000000000100", str(nudge["text"]))
        self.assertTrue(str(nudge["client_msg_id"]))

    def test_stale_action_cannot_mutate_resurfaced_card_and_drop_prevents_nudge(self) -> None:
        slack = FakeSlack()
        with tempfile.TemporaryDirectory() as directory:
            with SlackService(slack, FakeStore(), ALLOWLIST, Path(directory) / "inbox.sqlite3") as service:
                card = self.create(service)
                service.handle_interaction(card_payload(card.id, "director_later", "1710000020.000100", defer_seconds=86400))
                due_at = service._get_inbox_card(card.id).revisit_at
                self.assertEqual(service.resurface_due_inbox_cards(now=(due_at or 0) + 1), 1)
                self.assertIsNone(service.handle_interaction(card_payload(card.id, "director_drop", "1710000021.000100", version=2)))
                dropped = service.handle_interaction(card_payload(card.id, "director_drop", "1710000022.000100", version=3))
                self.assertIsNotNone(dropped)
                self.assertEqual(dropped.card.state, "dropped")
                self.assertEqual(service.reconcile_pending_inbox_card_renders(), 1)
                self.assertEqual(service.deliver_pending_inbox_card_nudges(), 0)

        self.assertEqual(len(slack.posts), 1)

    def test_uncertain_resurface_nudge_keeps_its_payload_and_never_reposts_on_restart(self) -> None:
        slack = FakeSlack()
        slack.history_pages = [{"ok": True, "channel": "C-allowed", "messages": []}]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "inbox.sqlite3"
            with SlackService(slack, FakeStore(), ALLOWLIST, path) as service:
                card = self.create(service)
                service.handle_interaction(card_payload(card.id, "director_later", "1710000030.000100", defer_seconds=86400))
                due_at = service._get_inbox_card(card.id).revisit_at
                self.assertEqual(service.resurface_due_inbox_cards(now=(due_at or 0) + 1), 1)
                self.assertEqual(service.reconcile_pending_inbox_card_renders(), 1)
                slack.fail_post = True
                self.assertEqual(service.deliver_pending_inbox_card_nudges(), 0)
                nudge_key = service._card_nudge_key(card.id)
                first = service._get_outbox(nudge_key or "")

            slack.fail_post = False
            with SlackService(slack, FakeStore(), ALLOWLIST, path) as reopened:
                self.assertEqual(reopened.deliver_pending_inbox_card_nudges(), 0)
                repeated = reopened._get_outbox(nudge_key or "")

        self.assertEqual(first.text, repeated.text)
        self.assertEqual(first.state, "pending_uncertain")
        self.assertEqual(repeated.state, "pending_uncertain")
        self.assertEqual(len(slack.posts), 2)
        self.assertIn("<@U-owner>", first.text)

    def test_stale_card_render_cannot_clear_a_newer_pending_render(self) -> None:
        slack = FakeSlack()
        with tempfile.TemporaryDirectory() as directory:
            with SlackService(slack, FakeStore(), ALLOWLIST, Path(directory) / "inbox.sqlite3") as service:
                card = self.create(service)
                service.handle_interaction(card_payload(card.id, "director_later", "1710000040.000100", defer_seconds=86400))
                original_update = service._update_inbox_card

                def update_then_newer_state(stale_card):
                    original_update(stale_card)
                    service._connection.execute(
                        """
                        UPDATE slack_inbox_cards
                        SET state = 'pending', revisit_at = NULL, version = version + 1,
                            render_state = 'pending'
                        WHERE id = ?
                        """,
                        (stale_card.id,),
                    )

                service._update_inbox_card = update_then_newer_state
                self.assertEqual(service.reconcile_pending_inbox_card_renders(), 0)
                current = service._get_inbox_card(card.id)
                self.assertEqual(current.version, 3)
                service._update_inbox_card = original_update
                self.assertEqual(service.reconcile_pending_inbox_card_renders(), 1)

        self.assertEqual(len(slack.updates), 2)

    def test_test_only_defer_requires_a_labeled_synthetic_card_and_short_due_time(self) -> None:
        slack = FakeSlack()
        with tempfile.TemporaryDirectory() as directory:
            with SlackService(slack, FakeStore(), ALLOWLIST, Path(directory) / "inbox.sqlite3") as service:
                ordinary = self.create(service)
                with self.assertRaises(ValueError):
                    service.test_defer_inbox_card(ordinary.idempotency_key, 110, now=100)
                synthetic = service.create_inbox_card(
                    "TEST: Resurface card",
                    "Synthetic test only.",
                    "https://director.test/archives/C-allowed/p1710000000000100?thread_ts=1710000000.000100&cid=C-allowed",
                    idempotency_key="test-resurface-card",
                )
                deferred = service.test_defer_inbox_card("test-resurface-card", 110, now=100)
                self.assertEqual(deferred.state, "deferred")
                self.assertEqual(deferred.version, 2)
                self.assertEqual(service.resurface_due_inbox_cards(now=110), 1)
                resurfaced = service._get_inbox_card(synthetic.id)
                self.assertEqual(resurfaced.state, "pending")
                self.assertEqual(resurfaced.version, 3)
                with self.assertRaises(ValueError):
                    service.test_defer_inbox_card("test-resurface-card", 4000, now=100)

    def test_existing_card_database_gains_resurface_columns(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "inbox.sqlite3"
            with sqlite3.connect(path) as connection:
                connection.execute(
                    """
                    CREATE TABLE slack_inbox_cards (
                        id TEXT PRIMARY KEY, idempotency_key TEXT NOT NULL UNIQUE,
                        client_msg_id TEXT NOT NULL UNIQUE, channel_id TEXT NOT NULL,
                        conversation_url TEXT NOT NULL, title TEXT NOT NULL, body TEXT NOT NULL,
                        state TEXT NOT NULL, revisit_at REAL, version INTEGER NOT NULL,
                        delivery_state TEXT NOT NULL, slack_ts TEXT, render_state TEXT NOT NULL,
                        created_at REAL NOT NULL, updated_at REAL NOT NULL
                    )
                    """
                )
            with SlackService(FakeSlack(), FakeStore(), ALLOWLIST, path) as service:
                columns = {
                    row["name"]
                    for row in service._connection.execute("PRAGMA table_info(slack_inbox_cards)").fetchall()
                }

        self.assertTrue({"resurface_cycle", "resurface_nudge_key", "resurface_nudge_sent_at"} <= columns)

    def test_rejects_card_targeting_a_different_workspace_or_channel(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            with SlackService(FakeSlack(), FakeStore(), ALLOWLIST, Path(directory) / "inbox.sqlite3") as service:
                with self.assertRaises(ValueError):
                    service.create_inbox_card(
                        "Weekend plans",
                        "Ready.",
                        "https://other.test/archives/C-allowed/p1710000000000100?thread_ts=1710000000.000100&cid=C-allowed",
                        idempotency_key="wrong-workspace",
                    )


class SlackRecoveryTests(unittest.TestCase):
    def test_recovers_paginated_history_and_all_tracked_threads_with_canonical_edit_revision(self) -> None:
        slack = FakeSlack()
        slack.history_pages = [
            {
                "ok": True,
                "channel": "C-allowed",
                "messages": [
                    {"ts": "1710000000.000100", "user": "U-owner"},
                    {"ts": "1710000002.000100", "user": "U-bot", "bot_id": "B-1"},
                ],
                "response_metadata": {"next_cursor": "1"},
            },
            {
                "ok": True,
                "channel": "C-allowed",
                "messages": [
                    {
                        "ts": "1710000001.000100",
                        "user": "U-owner",
                        "edited": {"user": "U-owner", "ts": "1710000003.000100"},
                    }
                ],
            },
        ]
        slack.reply_pages["1710000000.000100"] = [
            {"ok": True, "channel": "C-allowed", "messages": [{"ts": "1710000000.000100", "user": "U-owner"}]}
        ]
        slack.reply_pages["1710000001.000100"] = [
            {
                "ok": True,
                "channel": "C-allowed",
                "messages": [
                    {
                        "ts": "1710000001.000100",
                        "user": "U-owner",
                        "edited": {"user": "U-owner", "ts": "1710000003.000100"},
                    },
                    {"ts": "1710000004.000100", "thread_ts": "1710000001.000100", "user": "U-owner"},
                ],
            }
        ]
        tracked = SourceThread(thread_ts="1710000001.000100")
        store = FakeStore(threads=(tracked,))
        with tempfile.TemporaryDirectory() as directory:
            with SlackService(slack, store, ALLOWLIST, Path(directory) / "outbox.sqlite3") as service:
                self.assertEqual(service.recover_owner_messages(), 3)
                # Same deterministic recovery IDs make a repeated scan safe for a
                # real InboxStore, and are stable here as well.
                ids = [item.event_id for item in store.ingested]

        self.assertEqual({item.source_ts for item in store.ingested}, {"1710000000.000100", "1710000001.000100", "1710000004.000100"})
        changed = next(item for item in store.ingested if item.source_ts == "1710000001.000100")
        self.assertEqual(changed.event_ts, "1710000003.000100")
        self.assertTrue(all(item.event_id.startswith("recovery:") for item in store.ingested))
        self.assertEqual(len(ids), len(set(ids)))

    def test_recovery_scans_confirmed_outbox_root_for_owner_replies(self) -> None:
        slack = FakeSlack()
        slack.history_pages = [
            {"ok": True, "channel": "C-allowed", "messages": [{"ts": "1720000000.000100", "bot_id": "B-director"}]}
        ]
        slack.reply_pages["1720000000.000100"] = [
            {
                "ok": True,
                "channel": "C-allowed",
                "messages": [
                    {"ts": "1720000000.000100", "bot_id": "B-director"},
                    {"ts": "1720000001.000100", "thread_ts": "1720000000.000100", "user": "U-owner"},
                ],
            }
        ]
        store = FakeStore()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "outbox.sqlite3"
            with SlackService(slack, store, ALLOWLIST, path) as service:
                service.send_outgoing("question", idempotency_key="outbox-root")
                self.assertEqual(service.recover_owner_messages(), 1)

        self.assertEqual(store.ingested[0].source_ts, "1720000001.000100")

    def test_recovery_self_referential_root_thread_ts_does_not_reopen_message(self) -> None:
        slack = FakeSlack()
        root_ts = "1710000000.000100"
        slack.history_pages = [
            {
                "ok": True,
                "channel": "C-allowed",
                "messages": [
                    {"ts": root_ts, "thread_ts": root_ts, "reply_count": 1, "user": "U-owner"}
                ],
            }
        ]
        slack.reply_pages[root_ts] = [
            {
                "ok": True,
                "channel": "C-allowed",
                "messages": [{"ts": root_ts, "thread_ts": root_ts, "reply_count": 1, "user": "U-owner"}],
            }
        ]
        payload = {
            "team_id": "T-allowed",
            "event_id": "Ev-socket-root",
            "event": {"type": "message", "channel": "C-allowed", "user": "U-owner", "ts": root_ts},
        }
        pointer = normalize_event(payload, ALLOWLIST)
        assert pointer is not None
        with tempfile.TemporaryDirectory() as directory:
            inbox_path = Path(directory) / "inbox.sqlite3"
            with InboxStore(inbox_path) as inbox:
                initial = inbox.ingest(pointer).message
                with SlackService(slack, inbox, ALLOWLIST, Path(directory) / "outbox.sqlite3") as service:
                    service.recover_owner_messages()
                current = inbox.get_message(initial.id)

        self.assertIsNone(initial.thread_ts)
        self.assertIsNone(current.thread_ts)
        self.assertEqual(current.revision, 1)


if __name__ == "__main__":
    unittest.main()
