from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

from director.reminders import ReminderConflict, ReminderLeaseLost, ReminderStore


class ReminderStoreTests(unittest.TestCase):
    def test_create_is_idempotent_and_conflicting_reuse_is_rejected(self) -> None:
        with ReminderStore(":memory:") as reminders:
            first = reminders.create("daily-plan", "Review the plan", 100.0, thread_ts="1710000000.000100", now=1.0)
            repeated = reminders.create("daily-plan", "Review the plan", 100.0, thread_ts="1710000000.000100", now=2.0)

            self.assertEqual(repeated, first)
            self.assertEqual(first.service_idempotency_key, "reminder:daily-plan")
            with self.assertRaises(ReminderConflict):
                reminders.create("daily-plan", "Different reminder", 100.0, now=3.0)

    def test_restart_preserves_due_work_and_sent_receipt(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "director.sqlite3"
            with ReminderStore(path) as reminders:
                reminders.create("r-1", "Follow up", 100.0, now=1.0)

            with ReminderStore(path) as reopened:
                self.assertEqual([item.id for item in reopened.list_due(now=100.0)], ["r-1"])
                lease = reopened.claim_due("worker", 30, now=100.0)[0]
                reopened.mark_sent(lease, now=101.0)

            with ReminderStore(path) as reopened_again:
                self.assertEqual(reopened_again.list_due(now=200.0), ())
                self.assertEqual(reopened_again.get_sent_at("r-1"), 101.0)

    def test_expired_lease_retries_with_same_service_idempotency_key_and_fences_stale_worker(self) -> None:
        with ReminderStore(":memory:") as reminders:
            reminders.create("r-1", "Follow up", 100.0, now=1.0)
            first = reminders.claim_due("worker-a", 10, now=100.0)[0]
            self.assertEqual(reminders.claim_due("worker-b", 10, now=105.0), ())

            retry = reminders.claim_due("worker-b", 10, now=110.0)[0]
            self.assertEqual(first.service_idempotency_key, retry.service_idempotency_key)
            with self.assertRaises(ReminderLeaseLost):
                reminders.mark_sent(first, now=110.0)
            reminders.mark_sent(retry, now=111.0)
            self.assertEqual(reminders.list_due(now=111.0), ())

    def test_rejects_nonfinite_lease_duration(self) -> None:
        with ReminderStore(":memory:") as reminders:
            reminders.create("r-1", "Follow up", 100.0, now=1.0)
            for duration in (float("nan"), float("inf"), float("-inf")):
                with self.subTest(duration=duration):
                    with self.assertRaises(ValueError):
                        reminders.claim_due("worker", duration, now=100.0)

    def test_sent_receipt_is_marked_once(self) -> None:
        with ReminderStore(":memory:") as reminders:
            reminders.create("r-1", "Follow up", 100.0, now=1.0)
            lease = reminders.claim_due("worker", 10, now=100.0)[0]
            reminders.mark_sent(lease, now=101.0)

            with self.assertRaises(ReminderLeaseLost):
                reminders.mark_sent(lease, now=102.0)
            self.assertEqual(reminders.get_sent_at("r-1"), 101.0)
            self.assertEqual(reminders.list_due(now=200.0), ())
