"""Independent owner-review lifecycle checks, using isolated SQLite files."""
from dataclasses import replace
import hashlib
import json
from pathlib import Path
import tempfile
import time
from types import SimpleNamespace
import unittest

from director.dispatcher import Dispatcher
from director.inbox import InboxStore
from director.needs_you import NeedsYouError, NeedsYouHome, NeedsYouStore, NeedsYouTarget
import test_feed_integration as feed_test_support


class NeedsYouReviewTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "state.sqlite3"
        self.target = NeedsYouTarget("T-test", "U-owner", "C-source", "example.slack.com")
        self.store = NeedsYouStore(self.path, self.target)
        self.addCleanup(self.store.close)

    def add_item(self, key="result-1"):
        return self.store.record_completed_result(
            idempotency_key=key, root="123.000001",
            conversation_url="https://example.slack.com/archives/C-source/p123000001",
            title="Review the proposal", detail="Draft ready",
        )

    def test_result_recovery_does_not_reopen_completed_review(self):
        item = self.add_item()
        done, _ = self.store.set_done(item_id=item.id, expected_version=item.version,
                                     done=True, action_ts="100.000001")
        recovered = self.add_item()
        self.assertEqual((recovered.id, recovered.state, recovered.version),
                         (done.id, "done", done.version))

    def test_older_checkbox_cannot_undo_newer_snooze(self):
        item = self.add_item()
        snoozed = self.store.snooze(item_id=item.id, expected_version=item.version,
                                    until=time.time() + 3600)
        with self.assertRaises(NeedsYouError):
            self.store.set_done(item_id=item.id, expected_version=item.version,
                                done=True, action_ts="100.000002")
        self.assertEqual(self.store.get(item.id), snoozed)

    def test_duplicate_callback_cannot_recomplete_restored_item(self):
        item = self.add_item()
        done, _ = self.store.set_done(item_id=item.id, expected_version=item.version,
                                     done=True, action_ts="100.000003")
        active, _ = self.store.set_done(item_id=item.id, expected_version=done.version,
                                       done=False, action_ts="100.000004")
        self.store.set_done(item_id=item.id, expected_version=item.version,
                            done=True, action_ts="100.000003")
        self.assertEqual(self.store.get(item.id), active)

    def test_completed_state_survives_new_store_connection(self):
        item = self.add_item()
        done, _ = self.store.set_done(item_id=item.id, expected_version=item.version,
                                     done=True, action_ts="100.000005")
        reopened = NeedsYouStore(self.path, self.target)
        self.addCleanup(reopened.close)
        self.assertEqual(reopened.get(item.id), done)

    def test_source_reconfiguration_does_not_show_old_items(self):
        self.add_item()
        different_source = NeedsYouStore(self.path, replace(self.target, source_channel_id="C-other"))
        self.addCleanup(different_source.close)
        active, snoozed, done, *_ = different_source.list_for_home()
        self.assertEqual((active, snoozed, done), ([], [], []))

    def test_done_lists_only_latest_twenty_five(self):
        completed = []
        for index in range(28):
            item = self.add_item(f"result-{index}")
            self.store.set_done(item_id=item.id, expected_version=item.version,
                                done=True, action_ts=f"101.{index:06d}")
            completed.append(item.id)
        _, _, done, *_ = self.store.list_for_home()
        self.assertEqual([item.id for item in done], list(reversed(completed[-25:])))

    def test_due_snooze_survives_restart_and_only_resurfaces_once(self):
        item = self.add_item()
        due = time.time() + 3600
        snoozed = self.store.snooze(item_id=item.id, expected_version=item.version, until=due)
        reopened = NeedsYouStore(self.path, self.target)
        self.addCleanup(reopened.close)
        self.assertEqual(reopened.resurface_due(now=due - 1), 0)
        self.assertEqual(reopened.get(item.id).state, "snoozed")
        self.assertEqual(reopened.resurface_due(now=due + 1), 1)
        active = reopened.get(item.id)
        self.assertEqual(active.state, "active")
        self.assertGreater(active.version, snoozed.version)
        self.assertIsNone(active.snoozed_until)
        self.assertEqual(reopened.resurface_due(now=due + 2), 0)
        self.assertEqual(reopened.get(item.id), active)

    def test_done_item_cannot_resurface_from_its_old_snooze_time(self):
        item = self.add_item()
        due = time.time() + 3600
        snoozed = self.store.snooze(item_id=item.id, expected_version=item.version, until=due)
        done, _ = self.store.set_done(item_id=item.id, expected_version=snoozed.version,
                                     done=True, action_ts="102.000001")
        self.assertEqual(self.store.resurface_due(now=due + 1), 0)
        self.assertEqual(self.store.get(item.id), done)

    def home(self):
        home = NeedsYouHome(SimpleNamespace(), self.path, {
            "team_id": self.target.team_id, "owner_user_id": self.target.owner_user_id,
            "channel_id": self.target.source_channel_id, "workspace_domain": self.target.workspace_domain,
            "needs_you": {"enabled": True, "action_url": "https://bridge.example.test"},
        })
        self.addCleanup(home.close)
        return home

    def callback(self, home, action, user="U-owner"):
        return {"type": "block_actions", "team": {"id": "T-test"}, "user": {"id": user},
                "view": {"type": "home", "private_metadata": home.private_metadata()},
                "actions": [action]}

    def test_owner_checkbox_moves_item_to_done_and_wrong_user_cannot_restore(self):
        item = self.add_item()
        home = self.home()
        rendered = home.render()
        block = next(block for block in rendered["blocks"] if block.get("block_id", "").startswith("needs_you:"))
        element = block["elements"][0]
        action = {"block_id": block["block_id"], "action_id": element["action_id"],
                  "selected_options": element["options"], "action_ts": "103.000001"}
        self.assertTrue(home.handle_interaction(self.callback(home, action)))
        done = self.store.get(item.id)
        self.assertEqual(done.state, "done")
        action.update(block_id=f"needs_you:{item.id}:{done.version}", selected_options=[], action_ts="103.000002")
        self.assertFalse(home.handle_interaction(self.callback(home, action, user="U-other")))
        self.assertEqual(self.store.get(item.id), done)
        self.assertTrue(home.handle_interaction(self.callback(home, action)))
        self.assertEqual(self.store.get(item.id).state, "active")

    def test_pagination_makes_every_actionable_item_reachable(self):
        expected = {self.add_item(f"page-{index}").id for index in range(41)}
        home = self.home()
        seen = set()
        for _ in range(10):
            rendered = home.render()
            self.assertLessEqual(len(rendered["blocks"]), 100)
            seen.update(block["block_id"].split(":")[1] for block in rendered["blocks"]
                        if block.get("block_id", "").startswith("needs_you:"))
            more = [element for block in rendered["blocks"] for element in block.get("elements", [])
                    if element.get("action_id") == "needs_you_page" and element.get("value") == "next"]
            if not more:
                break
            self.assertTrue(home.handle_interaction(self.callback(home, more[0])))
        self.assertEqual(seen, expected)


class NeedsYouPublishReviewTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.project = Path(self.temp.name)
        self.path = self.project / "inbox.sqlite3"
        self.inbox = InboxStore(self.path)
        self.addCleanup(self.inbox.close)
        self.config = feed_test_support.config()
        self.config["needs_you"] = {"enabled": True, "action_url": "https://bridge.example.test"}
        self.home = NeedsYouHome(SimpleNamespace(), self.path, self.config)
        self.addCleanup(self.home.close)
        self.service = feed_test_support.ConfirmedReplyService()
        self.dispatcher = Dispatcher(self.project, self.config, self.inbox,
                                     self.service, needs_you=self.home)
        self.addCleanup(self.dispatcher.close)
        self.authority, self.job_key = feed_test_support.ConversationFeedIntegrationTests._active_source_authority(
            self, self.dispatcher)

    def test_reply_without_owner_action_does_not_create_review(self):
        result = self.dispatcher.publish_agent_reply(self.authority, "Sent the requested message.")
        self.assertEqual(result, {"published": True, "state": "sent"})
        active, snoozed, done, *_ = self.home.store.list_for_home()
        self.assertEqual((active, snoozed, done), ([], [], []))

    def test_explicit_review_is_created_once_and_checkbox_does_not_start_work(self):
        kwargs = {"needs_owner_action": {"title": "Review the proposal", "detail": "Draft ready"}}
        self.assertEqual(self.dispatcher.publish_agent_reply(self.authority, "Draft prepared.", **kwargs),
                         {"published": True, "state": "sent"})
        active, *_ = self.home.store.list_for_home()
        self.assertEqual(len(active), 1)
        jobs_before = [tuple(row) for row in self.dispatcher.db.execute("SELECT * FROM jobs")]
        self.home.store.set_done(item_id=active[0].id, expected_version=active[0].version,
                                 done=True, action_ts="200.000001")
        self.dispatcher.publish_agent_reply(self.authority, "Draft prepared.", **kwargs)
        self.dispatcher.recover_needs_you()
        active, snoozed, done, *_ = self.home.store.list_for_home()
        self.assertEqual((len(active), len(snoozed), len(done)), (0, 0, 1))
        self.assertEqual(len(self.service.sent), 1)
        self.assertEqual([tuple(row) for row in self.dispatcher.db.execute("SELECT * FROM jobs")], jobs_before)

    def test_legacy_conversation_metadata_digest_remains_retryable(self):
        metadata = {"conversation_title": "Proposal", "conversation_emoji": "📄",
                    "conversation_preview": "Prepared a proposal."}
        text = "The proposal is ready."
        historical_payload = {"text": text, "responsibility_id": None,
                              "execution_fence": None, **metadata}
        historical_digest = hashlib.sha256(json.dumps(historical_payload, sort_keys=True,
                                                    separators=(",", ":")).encode()).hexdigest()
        with self.dispatcher.db:
            self.dispatcher.db.execute("UPDATE agent_reply_authorities SET text_digest=? WHERE authority=?",
                                       (historical_digest, self.authority))
        self.assertEqual(self.dispatcher.publish_agent_reply(self.authority, text, **metadata),
                         {"published": True, "state": "sent"})


if __name__ == "__main__":
    unittest.main()
