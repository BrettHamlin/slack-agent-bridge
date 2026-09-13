from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

from director.responsibilities import ResponsibilityError, ResponsibilityStore
from director.slack_service import SlackService, SlackServiceError, _client_msg_id
from director.slack_transport import SlackAllowlist


ALLOWLIST = SlackAllowlist("T-allowed", "C-allowed", "U-owner", "director.test")
URL = "https://director.test/archives/C-allowed/p1710000000000100?thread_ts=1710000000.000100&cid=C-allowed"


class FakeStore:
    pass


class FakeSlack:
    def __init__(self) -> None:
        self.posts: list[dict[str, object]] = []
        self.updates: list[dict[str, object]] = []

    def chat_postMessage(self, **kwargs: object) -> dict[str, object]:
        self.posts.append(kwargs)
        return {"ok": True, "message": {"ts": "1720000000.000100"}}

    def chat_update(self, **kwargs: object) -> dict[str, object]:
        self.updates.append(kwargs)
        return {"ok": True, "ts": kwargs["ts"]}


def card_payload(card_id: str, action_id: str, action_ts: str, *, version: int = 1) -> dict[str, object]:
    action: dict[str, object] = {"action_id": action_id, "action_ts": action_ts, "value": f"{card_id}:{version}"}
    if action_id == "director_later":
        action["selected_option"] = {"value": f"{card_id}:{version}:86400"}
    return {
        "type": "block_actions", "team": {"id": "T-allowed"}, "channel": {"id": "C-allowed"},
        "user": {"id": "U-owner"}, "container": {"message_ts": "1720000000.000100"}, "actions": [action],
    }


class ResponsibilityStoreTests(unittest.TestCase):
    def test_waiting_card_is_repaired_once_and_drop_stays_dropped(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "director.sqlite3"
            slack = FakeSlack()
            with ResponsibilityStore(path) as store, SlackService(slack, FakeStore(), ALLOWLIST, path) as service:
                store.create("trip", "Plan trip", "Which dates?", URL, current_thread="1710000000.000100", now=100)
                store.create("active", "Active", "Work", URL, state="runnable", now=100)
                self.assertEqual(service.ensure_waiting_attention_cards(now=129), 0)
                self.assertEqual(service.ensure_waiting_attention_cards(now=131), 1)
                self.assertEqual(service.ensure_waiting_attention_cards(now=132), 0)
                self.assertEqual(len(slack.posts), 1)
                card_id = store.get("trip").attention_card_id
                self.assertIsNotNone(card_id)
                store.cancel("trip", now=133)
                self.assertEqual(service.ensure_waiting_attention_cards(now=200), 0)
                self.assertEqual(len(slack.posts), 1)


    def test_claim_expiry_reopens_runnable_and_fences_stale_completion_and_publication(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "director.sqlite3"
            with ResponsibilityStore(path) as store:
                created = store.create("call-mom", "Call Mom", "Prepare a time", URL, current_thread="1710000000.000100", state="runnable", now=100)
                self.assertEqual(created.state, "runnable")
                first = store.claim("call-mom", "worker-a", lease_seconds=10, now=100)
                self.assertTrue(store.execution_gate("call-mom", first.execution_fence, now=109))
                self.assertFalse(store.execution_gate("call-mom", first.execution_fence, now=110))
                self.assertEqual(store.list(runnable_only=True)[0].id, "call-mom")
                second = store.claim("call-mom", "worker-b", lease_seconds=10, now=111)
                self.assertNotEqual(first.execution_fence, second.execution_fence)
                self.assertFalse(store.record_publication("call-mom", first.execution_fence, "result-1", "old", now=111))
                with self.assertRaises(ResponsibilityError):
                    store.complete("call-mom", first.execution_fence, now=111)
                self.assertTrue(store.record_publication("call-mom", second.execution_fence, "result-2", "new", now=111))
                self.assertEqual(store.complete("call-mom", second.execution_fence, now=112).state, "completed")

    def test_context_rebind_retains_identity_and_history(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            with ResponsibilityStore(Path(directory) / "director.sqlite3") as store:
                store.create("same-task", "Follow up", "Read the reply", URL, source_message_id=7, source_revision=2, now=100)
                moved = store.update("same-task", conversation_url=URL.replace("000100", "000200", 1), current_thread="1710000001.000100", source_message_id=8, source_revision=1, now=101)
                self.assertEqual(moved.id, "same-task")
                self.assertEqual(moved.source_message_id, 8)
                history = store._connection.execute("SELECT event FROM responsibility_history WHERE responsibility_id = ? ORDER BY version", ("same-task",)).fetchall()
                self.assertEqual([row["event"] for row in history], ["created", "updated"])

    def test_update_revokes_claim_and_preserves_a_deferred_reconsideration(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            with ResponsibilityStore(Path(directory) / "director.sqlite3") as store:
                store.create("fenced-update", "Follow up", "Read", URL, current_thread="1710000000.000100", state="runnable", now=100)
                claim = store.claim("fenced-update", "worker", now=100)
                with self.assertRaises(ResponsibilityError):
                    store.update("fenced-update", current_thread="1710000001.000100", now=101)
                requeued = store.update("fenced-update", state="waiting_input", now=101)
                self.assertIsNone(requeued.execution_fence)
                self.assertFalse(store.execution_gate("fenced-update", claim.execution_fence, now=101))
                store._connection.execute("UPDATE responsibilities SET state = 'deferred', reconsider_at = 300 WHERE id = 'fenced-update'")
                updated = store.update("fenced-update", next_action="Wait for answer", now=102)
                self.assertEqual(updated.state, "deferred")
                self.assertEqual(updated.reconsider_at, 300)


class LinkedAttentionCardTests(unittest.TestCase):
    def test_later_due_drop_and_completion_sync_the_linked_responsibility(self) -> None:
        slack = FakeSlack()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "director.sqlite3"
            with ResponsibilityStore(path) as responsibilities:
                responsibilities.create("weekend", "Weekend plans", "Choose a time", URL, current_thread="1710000000.000100", now=100)
            with SlackService(slack, FakeStore(), ALLOWLIST, path) as service:
                card = service.create_inbox_card("Weekend plans", "Ready to choose.", URL, idempotency_key="weekend", responsibility_id="weekend")
                later = service.handle_interaction(card_payload(card.id, "director_later", "1710000001.000100"))
                self.assertIsNotNone(later)
                with ResponsibilityStore(path) as responsibilities:
                    deferred = responsibilities.get("weekend")
                    self.assertEqual(deferred.state, "deferred")
                    self.assertEqual(deferred.reconsider_at, later.card.revisit_at)
                    self.assertFalse(responsibilities.execution_gate("weekend", "anything"))
                service.resurface_due_inbox_cards(now=(later.card.revisit_at or 0) + 1)
                with ResponsibilityStore(path) as responsibilities:
                    self.assertEqual(responsibilities.get("weekend").state, "waiting_input")
                    self.assertIsNone(responsibilities.get("weekend").reconsider_at)
                    resumed = responsibilities.update("weekend", state="runnable")
                    claim = responsibilities.claim(resumed.id, "worker", now=(later.card.revisit_at or 0) + 2)
                    self.assertEqual(responsibilities.complete("weekend", claim.execution_fence, now=(later.card.revisit_at or 0) + 3).state, "completed")
                completed_card = service._get_inbox_card(card.id)
                self.assertEqual(completed_card.resolution_state, "completed")
                self.assertEqual(service.reconcile_pending_inbox_card_renders(), 1)

    def test_fenced_publication_posts_to_the_current_thread(self) -> None:
        slack = FakeSlack()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "director.sqlite3"
            with ResponsibilityStore(path) as responsibilities:
                responsibilities.create("publish", "Send result", "Send it", URL, current_thread="1710000000.000100", state="runnable")
                claim = responsibilities.claim("publish", "worker")
            with SlackService(slack, FakeStore(), ALLOWLIST, path) as service:
                delivery = service.publish_responsibility_result("publish", claim.execution_fence, "Prepared result.", idempotency_key="publish-1")
                self.assertIsNotNone(delivery)
                self.assertEqual(delivery.state, "sent")
                self.assertIsNone(service.publish_responsibility_result("publish", "stale", "Old result.", idempotency_key="publish-old"))
        self.assertEqual(slack.posts[-1]["thread_ts"], "1710000000.000100")

    def test_prepared_publication_retries_once_after_a_crash_before_dispatch_reservation(self) -> None:
        slack = FakeSlack()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "director.sqlite3"
            with ResponsibilityStore(path) as responsibilities:
                responsibilities.create("retry-publish", "Send result", "Send it", URL, current_thread="1710000000.000100", state="runnable")
                claim = responsibilities.claim("retry-publish", "worker")
            with SlackService(slack, FakeStore(), ALLOWLIST, path) as service:
                # Simulate a process that committed phase one then crashed.
                service._connection.execute("BEGIN IMMEDIATE")
                service._connection.execute(
                    "INSERT INTO responsibility_publications (responsibility_id, idempotency_key, execution_fence, summary, created_at) VALUES (?, ?, ?, ?, 1)",
                    ("retry-publish", "retry-key", claim.execution_fence, "Prepared result."),
                )
                _, created = service._prepare_outbox(
                    "retry-key", _client_msg_id(ALLOWLIST, "retry-key", "1710000000.000100"), "Prepared result.", "1710000000.000100"
                )
                self.assertTrue(created)
                service._connection.execute("COMMIT")
                first = service.publish_responsibility_result("retry-publish", claim.execution_fence, "Prepared result.", idempotency_key="retry-key")
                second = service.publish_responsibility_result("retry-publish", claim.execution_fence, "Prepared result.", idempotency_key="retry-key")
                self.assertIsNotNone(first)
                self.assertIsNotNone(second)
        self.assertEqual(len(slack.posts), 1)

    def test_result_key_cannot_reuse_an_unrelated_outbox_entry(self) -> None:
        slack = FakeSlack()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "director.sqlite3"
            with ResponsibilityStore(path) as responsibilities:
                responsibilities.create("collision", "Send result", "Send it", URL, current_thread="1710000000.000100", state="runnable")
                claim = responsibilities.claim("collision", "worker")
            with SlackService(slack, FakeStore(), ALLOWLIST, path) as service:
                service.send_outgoing("Unrelated.", idempotency_key="collision-key")
                with self.assertRaises(SlackServiceError):
                    service.publish_responsibility_result("collision", claim.execution_fence, "Result.", idempotency_key="collision-key")
        self.assertEqual(len(slack.posts), 1)

    def test_continued_card_detaches_so_a_later_attention_cycle_can_link_another_card(self) -> None:
        slack = FakeSlack()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "director.sqlite3"
            with ResponsibilityStore(path) as responsibilities:
                responsibilities.create("cycle", "Cycle", "Ask", URL)
            with SlackService(slack, FakeStore(), ALLOWLIST, path) as service:
                first = service.create_inbox_card("Cycle", "First.", URL, idempotency_key="cycle-a", responsibility_id="cycle", responsibility_version=1)
                with ResponsibilityStore(path) as responsibilities:
                    continued = responsibilities.update("cycle", state="runnable")
                    self.assertIsNone(continued.attention_card_id)
                    waiting = responsibilities.update("cycle", state="waiting_input")
                second = service.create_inbox_card("Cycle", "Second.", URL, idempotency_key="cycle-b", responsibility_id="cycle", responsibility_version=waiting.version)
                self.assertEqual(service._get_inbox_card(first.id).resolution_state, "continued")
                self.assertEqual(service._get_inbox_card(second.id).state, "pending")

    def test_drop_cancels_and_blocks_a_claimed_worker(self) -> None:
        slack = FakeSlack()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "director.sqlite3"
            with ResponsibilityStore(path) as responsibilities:
                responsibilities.create("drop-me", "Drop me", "Do the work", URL, current_thread="1710000000.000100", state="waiting_input")
            with SlackService(slack, FakeStore(), ALLOWLIST, path) as service:
                card = service.create_inbox_card("Drop me", "Ready.", URL, idempotency_key="drop-me", responsibility_id="drop-me")
                with ResponsibilityStore(path) as responsibilities:
                    # Simulate a legacy worker claim that exists while a card
                    # is still visible; the callback must revoke it atomically.
                    responsibilities._connection.execute(
                        "UPDATE responsibilities SET state = 'claimed', execution_fence = 'legacy-fence', claim_expires_at = 9999999999 WHERE id = 'drop-me'"
                    )
                dropped = service.handle_interaction(card_payload(card.id, "director_drop", "1710000002.000100"))
                self.assertIsNotNone(dropped)
                with ResponsibilityStore(path) as responsibilities:
                    self.assertEqual(responsibilities.get("drop-me").state, "cancelled")
                    self.assertFalse(responsibilities.execution_gate("drop-me", "legacy-fence"))
                    with self.assertRaises(ResponsibilityError):
                        responsibilities.complete("drop-me", "legacy-fence")
