from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

from director.inbox import InboxStore, InboundPointer, LeaseLost


def pointer(
    event_id: str = "Ev-1", *, event_ts: str = "1710000000.000100", thread_ts: str | None = "1710000000.000100"
) -> InboundPointer:
    return InboundPointer(
        event_id=event_id,
        source_team_id="T0000000009",
        source_channel_id="C0000000003",
        source_ts="1710000000.000100",
        event_ts=event_ts,
        thread_ts=thread_ts,
        received_at=100.0,
    )


class InboxStoreTests(unittest.TestCase):
    def test_deleted_revision_rejects_previous_read_and_completion(self) -> None:
        with InboxStore(':memory:') as inbox:
            original = pointer()
            message = inbox.ingest(original).message
            inbox.mark_read_if_revision(message.id, 1)
            deleted = InboundPointer('Ev-delete', original.source_team_id,
                original.source_channel_id, original.source_ts, '1710000003.000100',
                original.thread_ts, event_type='message_deleted')
            updated = inbox.ingest(deleted).message
            self.assertEqual(updated.revision, 2)
            self.assertFalse(inbox.mark_read_if_revision(message.id, 1))
            self.assertFalse(inbox.mark_completed_if_revision(message.id, 1))
            self.assertEqual(inbox.get_message(message.id).event_type, 'message_deleted')

    def test_reopen_keeps_source_pointer_and_ingested_receipt(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "inbox.sqlite3"
            with InboxStore(path) as inbox:
                result = inbox.ingest(pointer())
                self.assertTrue(result.created)
                message_id = result.message.id

            with InboxStore(path) as reopened:
                saved = reopened.get_message(message_id)
                self.assertEqual(saved.source_channel_id, "C0000000003")
                self.assertEqual(saved.thread_ts, "1710000000.000100")
                self.assertEqual(reopened.get_receipts(message_id).ingested_at, 100.0)
                self.assertEqual(reopened.pending_relay_count(now=101.0), 1)

    def test_new_source_queues_intake_ack_without_marking_read_or_complete(self) -> None:
        with InboxStore(":memory:") as inbox:
            message_id = inbox.ingest(pointer()).message.id

            self.assertEqual(inbox.intake_ack_state(message_id), "pending")
            receipts = inbox.get_receipts(message_id)
            self.assertIsNone(receipts.read_at)
            self.assertIsNone(receipts.completed_at)

    def test_intake_ack_requeues_current_edit_and_skips_deleted_or_cross_channel_sources(self) -> None:
        with InboxStore(":memory:") as inbox:
            superseded = inbox.ingest(pointer()).message
            old_lease = inbox.claim_pending_intake_acks(
                "worker-a", 10, superseded.source_team_id, superseded.source_channel_id, now=100.0
            )[0]
            inbox.ingest(pointer("Ev-edit", event_ts="1710000002.000300"))
            current_lease = inbox.claim_pending_intake_acks(
                "worker-b", 10, superseded.source_team_id, superseded.source_channel_id, now=101.0
            )[0]
            self.assertEqual(current_lease.revision, 2)
            self.assertFalse(inbox.acknowledge_intake_ack(old_lease, now=101.0))
            self.assertTrue(inbox.acknowledge_intake_ack(current_lease, now=102.0))

            deleted = inbox.ingest(
                InboundPointer(
                    "Ev-delete-source", "T0000000009", "C0000000003", "1710000100.000100",
                    "1710000100.000100", event_type="message_deleted", received_at=100.0,
                )
            ).message
            self.assertIsNone(inbox.intake_ack_state(deleted.id))

            cross_channel = inbox.ingest(
                InboundPointer(
                    "Ev-cross", "T0000000009", "C0000000003", "1710000200.000100",
                    "1710000200.000100", received_at=100.0,
                )
            ).message
            self.assertEqual(
                inbox.claim_pending_intake_acks("worker", 10, "T-other", "C-other", now=110.0), (),
            )
            self.assertEqual(inbox.intake_ack_state(cross_channel.id), "skipped")

    def test_expired_intake_ack_lease_is_recovered(self) -> None:
        with InboxStore(":memory:") as inbox:
            message = inbox.ingest(pointer()).message
            first = inbox.claim_pending_intake_acks(
                "worker-a", 10, message.source_team_id, message.source_channel_id, now=100.0
            )[0]
            second = inbox.claim_pending_intake_acks(
                "worker-b", 10, message.source_team_id, message.source_channel_id, now=110.0
            )[0]

            self.assertNotEqual(first.token, second.token)
            self.assertFalse(inbox.acknowledge_intake_ack(first, now=110.0))
            self.assertTrue(inbox.acknowledge_intake_ack(second, now=111.0))
            self.assertEqual(inbox.intake_ack_state(message.id), "sent")

    def test_dedupes_event_and_advances_stable_source_on_update(self) -> None:
        with InboxStore(":memory:") as inbox:
            first = inbox.ingest(pointer())
            duplicate = inbox.ingest(pointer())
            updated = inbox.ingest(pointer("Ev-2", event_ts="1710000001.000200", thread_ts="1710000000.000100"))

            self.assertTrue(duplicate.duplicate_event)
            self.assertFalse(duplicate.source_updated)
            self.assertTrue(updated.source_updated)
            self.assertEqual(updated.message.id, first.message.id)
            self.assertEqual(updated.message.revision, 2)
            self.assertEqual(updated.message.event_ts, "1710000001.000200")

    def test_expired_lease_can_be_reclaimed_and_old_holder_is_fenced(self) -> None:
        with InboxStore(":memory:") as inbox:
            message_id = inbox.ingest(pointer()).message.id
            first = inbox.claim_pending_relay("worker-a", 10, now=100.0)[0]
            self.assertEqual(inbox.claim_pending_relay("worker-b", 10, now=105.0), ())

            second = inbox.claim_pending_relay("worker-b", 10, now=110.0)[0]
            with self.assertRaises(LeaseLost):
                inbox.acknowledge_relay(first, now=110.0)
            inbox.acknowledge_relay(second, now=111.0)
            self.assertEqual(inbox.pending_relay_count(now=111.0), 0)
            self.assertTrue(inbox.get_receipts(message_id).is_relayed(1))

    def test_rejects_nonfinite_lease_duration(self) -> None:
        with InboxStore(":memory:") as inbox:
            inbox.ingest(pointer())
            for duration in (float("nan"), float("inf"), float("-inf")):
                with self.subTest(duration=duration):
                    with self.assertRaises(ValueError):
                        inbox.claim_pending_relay("worker", duration)

    def test_source_update_requeues_current_revision_and_fences_old_lease(self) -> None:
        with InboxStore(":memory:") as inbox:
            message_id = inbox.ingest(pointer()).message.id
            stale = inbox.claim_pending_relay("worker-a", 30, now=100.0)[0]
            update = inbox.ingest(pointer("Ev-2", event_ts="1710000002.000300"))

            self.assertEqual(update.message.id, message_id)
            self.assertEqual(update.message.revision, 2)
            with self.assertRaises(LeaseLost):
                inbox.acknowledge_relay(stale, now=101.0)
            current = inbox.claim_pending_relay("worker-b", 30, now=101.0)[0]
            self.assertEqual(current.revision, 2)

    def test_out_of_order_original_does_not_regress_newer_edit(self) -> None:
        with InboxStore(":memory:") as inbox:
            newer = inbox.ingest(pointer("Ev-edit", event_ts="1710000000.900000"))
            stale = inbox.ingest(pointer("Ev-original", event_ts="1710000000.100000", thread_ts=None))

            self.assertFalse(stale.source_updated)
            self.assertEqual(stale.message.id, newer.message.id)
            self.assertEqual(stale.message.revision, 1)
            self.assertEqual(stale.message.event_ts, "1710000000.900000")
            self.assertEqual(stale.message.thread_ts, "1710000000.000100")

    def test_edit_clears_current_read_and_completion_state(self) -> None:
        with InboxStore(":memory:") as inbox:
            message_id = inbox.ingest(pointer()).message.id
            inbox.mark_read(message_id, now=120.0)
            inbox.mark_completed(message_id, now=121.0)
            inbox.ingest(pointer("Ev-edit", event_ts="1710000002.000300"))

            revised = inbox.get_receipts(message_id)
            self.assertIsNone(revised.read_at)
            self.assertIsNone(revised.completed_at)
            self.assertEqual(revised.read_revisions, (1,))
            self.assertEqual(revised.completed_revisions, (1,))
            self.assertTrue(inbox.mark_completed(message_id, now=122.0))
            self.assertEqual(inbox.get_receipts(message_id).completed_at, 122.0)

    def test_revision_fenced_terminal_receipts_reject_stale_worker(self) -> None:
        with InboxStore(":memory:") as inbox:
            message_id = inbox.ingest(pointer()).message.id
            inbox.ingest(pointer("Ev-edit", event_ts="1710000002.000300"))

            self.assertFalse(inbox.mark_read_if_revision(message_id, 1, now=120.0))
            self.assertFalse(inbox.mark_completed_if_revision(message_id, 1, now=121.0))
            self.assertTrue(inbox.mark_read_if_revision(message_id, 2, now=122.0))
            self.assertTrue(inbox.mark_completed_if_revision(message_id, 2, now=123.0))

            receipts = inbox.get_receipts(message_id)
            self.assertEqual(receipts.read_at, 122.0)
            self.assertEqual(receipts.completed_at, 123.0)
            self.assertEqual(receipts.read_revisions, (2,))
            self.assertEqual(receipts.completed_revisions, (2,))

    def test_lists_pending_pointers_and_every_observed_thread_root(self) -> None:
        with InboxStore(":memory:") as inbox:
            top_level = inbox.ingest(pointer("Ev-top", thread_ts=None)).message
            reply = inbox.ingest(
                InboundPointer(
                    event_id="Ev-reply",
                    source_team_id="T0000000009",
                    source_channel_id="C0000000003",
                    source_ts="1710000010.000100",
                    event_ts="1710000010.000100",
                    thread_ts="1710000005.000100",
                    received_at=105.0,
                )
            ).message
            lease = inbox.claim_pending_relay("worker-a", 30, now=110.0)[0]

            pending = inbox.list_pending_relays(now=111.0)
            self.assertEqual({item.message.id for item in pending}, {top_level.id, reply.id})
            leased = next(item for item in pending if item.message.id == lease.message_id)
            self.assertEqual(leased.lease_holder, "worker-a")
            self.assertEqual(inbox.list_pending_relays(include_leased=False, now=111.0), (pending[1],))
            self.assertEqual(
                {(item.source_channel_id, item.thread_ts) for item in inbox.list_source_threads()},
                {
                    ("C0000000003", "1710000000.000100"),
                    ("C0000000003", "1710000005.000100"),
                },
            )

    def test_checkpoint_updates_value_without_resetting_creation_time(self) -> None:
        with InboxStore(":memory:") as inbox:
            self.assertIsNone(inbox.get_checkpoint("slack-history"))
            first = inbox.set_checkpoint("slack-history", "cursor-1", now=100.0)
            second = inbox.set_checkpoint("slack-history", "cursor-2", now=120.0)

            self.assertEqual(first.created_at, 100.0)
            self.assertEqual(second.created_at, 100.0)
            self.assertEqual(second.updated_at, 120.0)
            self.assertEqual(inbox.get_checkpoint("slack-history"), second)

    def test_open_messages_remain_after_read_and_relay_until_completed(self) -> None:
        with InboxStore(":memory:") as inbox:
            message_id = inbox.ingest(pointer()).message.id
            inbox.mark_read(message_id, now=101.0)
            lease = inbox.claim_pending_relay("worker", 30, now=102.0)[0]
            inbox.acknowledge_relay(lease, now=103.0)

            self.assertEqual([item.id for item in inbox.list_open_messages()], [message_id])
            inbox.mark_completed(message_id, now=104.0)
            self.assertEqual(inbox.list_open_messages(), ())

    def test_read_and_completion_are_independent_receipts(self) -> None:
        with InboxStore(":memory:") as inbox:
            message_id = inbox.ingest(pointer()).message.id
            self.assertTrue(inbox.mark_completed(message_id, now=120.0))
            self.assertTrue(inbox.mark_read(message_id, now=121.0))
            self.assertFalse(inbox.mark_read(message_id, now=122.0))

            receipts = inbox.get_receipts(message_id)
            self.assertEqual(receipts.completed_at, 120.0)
            self.assertEqual(receipts.read_at, 121.0)
            self.assertFalse(receipts.is_relayed(1))

    def test_completed_unrelayed_current_revision_is_not_a_relay_candidate(self) -> None:
        with InboxStore(":memory:") as inbox:
            message_id = inbox.ingest(pointer()).message.id
            self.assertTrue(inbox.mark_completed(message_id, now=101.0))

            self.assertEqual(inbox.pending_relay_count(now=102.0), 0)
            self.assertEqual(inbox.list_pending_relays(now=102.0), ())
            self.assertEqual(inbox.claim_pending_relay("luna", 30, now=102.0), ())
            receipts = inbox.get_receipts(message_id)
            self.assertEqual(receipts.completed_revisions, (1,))
            self.assertEqual(receipts.relayed_revisions, ())

    def test_edit_reactivates_completed_unrelayed_source_revision(self) -> None:
        with InboxStore(":memory:") as inbox:
            message_id = inbox.ingest(pointer()).message.id
            inbox.mark_completed(message_id, now=101.0)
            updated = inbox.ingest(pointer("Ev-edit", event_ts="1710000002.000300"))

            self.assertEqual(updated.message.revision, 2)
            self.assertEqual(inbox.pending_relay_count(now=102.0), 1)
            self.assertEqual([item.message.id for item in inbox.list_pending_relays(now=102.0)], [message_id])
            lease = inbox.claim_pending_relay("luna", 30, now=102.0)[0]
            self.assertEqual(lease.revision, 2)

    def test_completion_after_relay_claim_allows_valid_acknowledgement(self) -> None:
        with InboxStore(":memory:") as inbox:
            message_id = inbox.ingest(pointer()).message.id
            lease = inbox.claim_pending_relay("luna", 30, now=100.0)[0]
            self.assertTrue(inbox.mark_completed_if_revision(message_id, lease.revision, now=101.0))

            inbox.acknowledge_relay(lease, now=102.0)
            receipts = inbox.get_receipts(message_id)
            self.assertEqual(receipts.completed_revisions, (1,))
            self.assertEqual(receipts.relayed_revisions, (1,))
            self.assertEqual(inbox.pending_relay_count(now=102.0), 0)
