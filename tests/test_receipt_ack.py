from __future__ import annotations

from pathlib import Path
import tempfile
import threading
import unittest

from director.inbox import InboxStore, InboundPointer
from director.receipt_ack import ReceiptAckWorker
from director.slack_transport import SlackAllowlist, SocketModeAck, make_socket_mode_listener


ALLOWLIST = SlackAllowlist("T-allowed", "C-allowed", "U-owner")


def pointer(event_id: str = "Ev-1") -> InboundPointer:
    return InboundPointer(
        event_id=event_id,
        source_team_id="T-allowed",
        source_channel_id="C-allowed",
        source_ts="1710000000.000100",
        event_ts="1710000000.000100",
        received_at=100.0,
    )


class ResponseData:
    def __init__(self, data: dict[str, object]) -> None:
        self.data = data


class AlreadyReacted(Exception):
    def __init__(self) -> None:
        self.response = ResponseData({"ok": False, "error": "already_reacted"})


class FakeSlack:
    def __init__(self) -> None:
        self.reactions: list[dict[str, object]] = []
        self.already_reacted = False
        self.fail = False

    def reactions_add(self, **kwargs: object) -> dict[str, object]:
        self.reactions.append(kwargs)
        if self.fail:
            raise RuntimeError("synthetic provider failure")
        if self.already_reacted:
            raise AlreadyReacted()
        return {"ok": True}


class BlockingSlack(FakeSlack):
    def __init__(self) -> None:
        super().__init__()
        self.started = threading.Event()
        self.release = threading.Event()

    def reactions_add(self, **kwargs: object) -> dict[str, object]:
        self.reactions.append(kwargs)
        self.started.set()
        self.release.wait(timeout=2.0)
        return {"ok": True}


class ReceiptAckWorkerTests(unittest.TestCase):
    def test_reacts_after_durable_intake_without_marking_read_or_complete(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "inbox.sqlite3"
            with InboxStore(path) as store:
                message_id = store.ingest(pointer()).message.id
            slack = FakeSlack()
            worker = ReceiptAckWorker(path, slack, ALLOWLIST)
            try:
                self.assertEqual(worker.process_once(), 1)
            finally:
                worker.close()
            with InboxStore(path) as store:
                self.assertEqual(store.intake_ack_state(message_id), "sent")
                receipts = store.get_receipts(message_id)
                self.assertIsNone(receipts.read_at)
                self.assertIsNone(receipts.completed_at)
        self.assertEqual(
            slack.reactions,
            [{"channel": "C-allowed", "timestamp": "1710000000.000100", "name": "white_check_mark"}],
        )

    def test_already_reacted_settles_the_durable_intake_ack(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "inbox.sqlite3"
            with InboxStore(path) as store:
                message_id = store.ingest(pointer()).message.id
            slack = FakeSlack()
            slack.already_reacted = True
            worker = ReceiptAckWorker(path, slack, ALLOWLIST)
            try:
                worker.process_once()
            finally:
                worker.close()
            with InboxStore(path) as store:
                self.assertEqual(store.intake_ack_state(message_id), "sent")

    def test_provider_failure_stays_durable_for_retry(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "inbox.sqlite3"
            with InboxStore(path) as store:
                message_id = store.ingest(pointer()).message.id
            slack = FakeSlack()
            slack.fail = True
            worker = ReceiptAckWorker(path, slack, ALLOWLIST)
            try:
                self.assertEqual(worker.process_once(), 1)
            finally:
                worker.close()
            with InboxStore(path) as store:
                self.assertEqual(store.intake_ack_state(message_id), "pending")

    def test_expired_lease_recovers_after_worker_interruption(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "inbox.sqlite3"
            with InboxStore(path) as store:
                message = store.ingest(pointer()).message
                store.claim_pending_intake_acks(
                    "interrupted", 10, message.source_team_id, message.source_channel_id, now=100.0
                )
            slack = FakeSlack()
            worker = ReceiptAckWorker(path, slack, ALLOWLIST)
            try:
                self.assertEqual(worker.process_once(), 1)
            finally:
                worker.close()
            with InboxStore(path) as store:
                self.assertEqual(store.intake_ack_state(message.id), "sent")

    def test_edit_or_delete_after_claim_is_not_reacted(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "inbox.sqlite3"
            for event_type in ("message_changed", "message_deleted"):
                with self.subTest(event_type=event_type):
                    with InboxStore(path) as store:
                        original = store.ingest(pointer(f"Ev-{event_type}")).message
                        slack = FakeSlack()
                        worker = ReceiptAckWorker(path, slack, ALLOWLIST)
                        try:
                            lease = worker._store.claim_pending_intake_acks(
                                worker._holder, 10, original.source_team_id, original.source_channel_id
                            )[0]
                            store.ingest(
                                InboundPointer(
                                    f"Ev-{event_type}-next",
                                    original.source_team_id,
                                    original.source_channel_id,
                                    original.source_ts,
                                    "1710000001.000100",
                                    event_type=event_type,
                                )
                            )
                            worker._react_or_retry(lease)
                            self.assertEqual(slack.reactions, [])
                            expected = "pending" if event_type == "message_changed" else "skipped"
                            self.assertEqual(store.intake_ack_state(original.id), expected)
                        finally:
                            worker.close()

    def test_background_reaction_does_not_delay_socket_mode_ack(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "inbox.sqlite3"
            slack = BlockingSlack()
            worker = ReceiptAckWorker(path, slack, ALLOWLIST, poll_seconds=0.01)
            worker.start()
            responses: list[SocketModeAck] = []
            request = type(
                "Request",
                (),
                {
                    "type": "events_api",
                    "envelope_id": "envelope-1",
                    "payload": {
                        "team_id": "T-allowed",
                        "event_id": "Ev-background",
                        "event": {
                            "type": "message",
                            "channel": "C-allowed",
                            "user": "U-owner",
                            "ts": "1710000000.000100",
                        },
                    },
                },
            )()
            try:
                with InboxStore(path) as store:
                    listener = make_socket_mode_listener(
                        store,
                        ALLOWLIST,
                        response_factory=SocketModeAck,
                        intake_ack_notifier=worker.wake,
                    )
                    client = type("Client", (), {"send_socket_mode_response": responses.append})()
                    listener(client, request)
                    self.assertEqual(responses, [SocketModeAck("envelope-1")])
                self.assertTrue(slack.started.wait(timeout=1.0))
            finally:
                slack.release.set()
                worker.close()


if __name__ == "__main__":
    unittest.main()
