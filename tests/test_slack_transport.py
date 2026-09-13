from __future__ import annotations

import unittest

from director.inbox import InboundPointer
from director.slack_transport import (
    SlackAllowlist,
    SlackSourceError,
    SocketModeAck,
    fetch_source_evidence,
    make_socket_mode_listener,
    normalize_event,
)


ALLOWLIST = SlackAllowlist(team_id="T-allowed", channel_id="C-allowed", owner_user_id="U-owner")


def payload(event: dict[str, object], event_id: str = "Ev-1", *, team_id: str = "T-allowed") -> dict[str, object]:
    return {"team_id": team_id, "event_id": event_id, "event": event}


def ordinary_event(**changes: object) -> dict[str, object]:
    event: dict[str, object] = {
        "type": "message",
        "channel": "C-allowed",
        "user": "U-owner",
        "ts": "1710000000.000100",
    }
    event.update(changes)
    return event


class FakeStore:
    def __init__(self, error: Exception | None = None) -> None:
        self.error = error
        self.pointers: list[InboundPointer] = []

    def ingest(self, pointer: InboundPointer) -> None:
        if self.error is not None:
            raise self.error
        self.pointers.append(pointer)


class FakeClient:
    def __init__(self) -> None:
        self.responses: list[SocketModeAck] = []

    def send_socket_mode_response(self, response: SocketModeAck) -> None:
        self.responses.append(response)


class FakeRequest:
    type = "events_api"

    def __init__(self, payload: dict[str, object], envelope_id: str = "envelope-1") -> None:
        self.payload = payload
        self.envelope_id = envelope_id


class InteractiveRequest(FakeRequest):
    type = "interactive"


class FakeInteractionHandler:
    def __init__(self, error: Exception | None = None) -> None:
        self.error = error
        self.payloads: list[dict[str, object]] = []

    def handle_interaction(self, payload: dict[str, object]) -> None:
        if self.error is not None:
            raise self.error
        self.payloads.append(payload)


class SlackResponseShape:
    """The relevant shape of slack_sdk.web.slack_response.SlackResponse."""

    def __init__(self, data: dict[str, object]) -> None:
        self.data = data


class FakeWebClient:
    def __init__(self, pages: list[dict[str, object]], *, sdk_response: bool = False) -> None:
        self.pages = pages
        self.sdk_response = sdk_response
        self.calls: list[dict[str, object]] = []

    def conversations_replies(self, **kwargs: object) -> dict[str, object] | SlackResponseShape:
        self.calls.append(kwargs)
        page = self.pages[len(self.calls) - 1]
        return SlackResponseShape(page) if self.sdk_response else page


class SlackNormalizationTests(unittest.TestCase):
    def test_wrong_team_channel_or_owner_is_dropped(self) -> None:
        self.assertIsNone(normalize_event(payload(ordinary_event(), team_id="T-other"), ALLOWLIST))
        self.assertIsNone(normalize_event(payload(ordinary_event(channel="C-other")), ALLOWLIST))
        self.assertIsNone(normalize_event(payload(ordinary_event(user="U-other")), ALLOWLIST))

    def test_ordinary_old_thread_and_edited_events_are_accepted(self) -> None:
        ordinary = normalize_event(payload(ordinary_event(), "Ev-ordinary"), ALLOWLIST)
        old_thread = normalize_event(
            payload(ordinary_event(ts="1600000000.000100", thread_ts="1500000000.000100"), "Ev-old"), ALLOWLIST
        )
        edited = normalize_event(
            payload(
                {
                    "type": "message",
                    "subtype": "message_changed",
                    "channel": "C-allowed",
                    "event_ts": "1999999999.999999",
                    "message": {
                        "type": "message",
                        "user": "U-owner",
                        "ts": "1710000000.000100",
                        "thread_ts": "1600000000.000100",
                        "edited": {"user": "U-owner", "ts": "1710000001.000200"},
                    },
                },
                "Ev-edit",
            ),
            ALLOWLIST,
        )

        self.assertEqual(ordinary.event_ts if ordinary else None, "1710000000.000100")
        self.assertEqual(old_thread.thread_ts if old_thread else None, "1500000000.000100")
        self.assertEqual(edited.source_ts if edited else None, "1710000000.000100")
        self.assertEqual(edited.event_ts if edited else None, "1710000001.000200")

    def test_only_explicit_non_system_subtypes_are_accepted(self) -> None:
        self.assertIsNotNone(
            normalize_event(payload(ordinary_event(subtype="file_share"), "Ev-file"), ALLOWLIST)
        )
        self.assertIsNotNone(
            normalize_event(payload(ordinary_event(subtype="thread_broadcast"), "Ev-broadcast"), ALLOWLIST)
        )
        self.assertIsNone(normalize_event(payload(ordinary_event(subtype="channel_join"), "Ev-system"), ALLOWLIST))
        self.assertIsNone(normalize_event(payload(ordinary_event(bot_id="B-1"), "Ev-bot"), ALLOWLIST))


class SlackSocketModeTests(unittest.TestCase):
    def test_persists_before_acknowledging(self) -> None:
        store = FakeStore()
        client = FakeClient()
        listener = make_socket_mode_listener(store, ALLOWLIST, response_factory=SocketModeAck)

        listener(client, FakeRequest(payload(ordinary_event())))

        self.assertEqual(len(store.pointers), 1)
        self.assertEqual(client.responses, [SocketModeAck("envelope-1")])

    def test_durable_intake_wakes_reaction_worker_without_waiting_for_slack(self) -> None:
        events: list[str] = []

        class OrderedStore(FakeStore):
            def ingest(self, pointer: InboundPointer) -> None:
                events.append("ingested")
                super().ingest(pointer)

        class OrderedClient(FakeClient):
            def send_socket_mode_response(self, response: SocketModeAck) -> None:
                events.append("acknowledged")
                super().send_socket_mode_response(response)

        listener = make_socket_mode_listener(
            OrderedStore(),
            ALLOWLIST,
            response_factory=SocketModeAck,
            intake_ack_notifier=lambda: events.append("woken"),
        )

        listener(OrderedClient(), FakeRequest(payload(ordinary_event())))

        self.assertEqual(events, ["ingested", "woken", "acknowledged"])

    def test_write_failure_leaves_event_unacknowledged(self) -> None:
        store = FakeStore(RuntimeError("database unavailable"))
        client = FakeClient()
        listener = make_socket_mode_listener(store, ALLOWLIST, response_factory=SocketModeAck)

        listener(client, FakeRequest(payload(ordinary_event())))

        self.assertEqual(client.responses, [])

    def test_dropped_event_is_acknowledged_without_persisting(self) -> None:
        store = FakeStore()
        client = FakeClient()
        listener = make_socket_mode_listener(store, ALLOWLIST, response_factory=SocketModeAck)

        listener(client, FakeRequest(payload(ordinary_event(user="U-other"))))

        self.assertEqual(store.pointers, [])
        self.assertEqual(client.responses, [SocketModeAck("envelope-1")])

    def test_interaction_is_dispatched_before_acknowledgement(self) -> None:
        store = FakeStore()
        client = FakeClient()
        handler = FakeInteractionHandler()
        listener = make_socket_mode_listener(store, ALLOWLIST, response_factory=SocketModeAck, interaction_handler=handler)
        request = InteractiveRequest({"type": "block_actions", "actions": []})

        listener(client, request)

        self.assertEqual(handler.payloads, [request.payload])
        self.assertEqual(client.responses, [SocketModeAck("envelope-1")])

    def test_interaction_failure_is_left_unacknowledged_for_retry(self) -> None:
        store = FakeStore()
        client = FakeClient()
        listener = make_socket_mode_listener(
            store,
            ALLOWLIST,
            response_factory=SocketModeAck,
            interaction_handler=FakeInteractionHandler(RuntimeError("durable update failed")),
        )

        listener(client, InteractiveRequest({"type": "block_actions", "actions": []}))

        self.assertEqual(client.responses, [])


class SlackSourceFetchTests(unittest.TestCase):
    def test_paginates_and_returns_untrusted_message_thread(self) -> None:
        client = FakeWebClient(
            [
                {
                    "ok": True,
                    "channel": "C-allowed",
                    "messages": [{"ts": "root", "user": "U-owner"}],
                    "response_metadata": {"next_cursor": "next"},
                },
                {
                    "ok": True,
                    "channel": {"id": "C-allowed"},
                    "messages": [
                        {"ts": "source", "thread_ts": "root", "user": "U-owner", "text": "private"}
                    ],
                    "response_metadata": {"next_cursor": ""},
                },
            ]
        )
        evidence = fetch_source_evidence(client, source_pointer(thread_ts="root"), ALLOWLIST)

        self.assertEqual(evidence.message["ts"], "source")
        self.assertEqual(len(evidence.thread), 2)
        self.assertEqual(client.calls[0]["channel"], "C-allowed")
        self.assertEqual(client.calls[0]["ts"], "root")
        self.assertEqual(client.calls[1]["cursor"], "next")

    def test_mismatched_source_owner_is_rejected(self) -> None:
        client = FakeWebClient(
            [{"ok": True, "channel": "C-allowed", "messages": [{"ts": "source", "user": "U-other"}]}]
        )

        with self.assertRaises(SlackSourceError):
            fetch_source_evidence(client, source_pointer(), ALLOWLIST)

    def test_accepts_slack_response_data_shape(self) -> None:
        client = FakeWebClient(
            [{"ok": True, "channel": "C-allowed", "messages": [{"ts": "source", "user": "U-owner"}]}],
            sdk_response=True,
        )

        evidence = fetch_source_evidence(client, source_pointer(), ALLOWLIST)

        self.assertEqual(evidence.message["ts"], "source")

    def test_rejects_source_from_a_different_thread_or_partial_thread(self) -> None:
        wrong_thread = FakeWebClient(
            [
                {
                    "ok": True,
                    "channel": "C-allowed",
                    "messages": [{"ts": "source", "thread_ts": "other", "user": "U-owner"}],
                }
            ]
        )
        partial_thread = FakeWebClient(
            [
                {
                    "ok": True,
                    "channel": "C-allowed",
                    "messages": [{"ts": "source", "user": "U-owner"}],
                    "has_more": True,
                }
            ]
        )

        with self.assertRaises(SlackSourceError):
            fetch_source_evidence(wrong_thread, source_pointer(thread_ts="root"), ALLOWLIST)
        with self.assertRaises(SlackSourceError):
            fetch_source_evidence(partial_thread, source_pointer(), ALLOWLIST)


def source_pointer(*, thread_ts: str | None = None) -> InboundPointer:
    return InboundPointer(
        event_id="Ev-source",
        source_team_id="T-allowed",
        source_channel_id="C-allowed",
        source_ts="source",
        event_ts="source",
        thread_ts=thread_ts,
    )


if __name__ == "__main__":
    unittest.main()
