"""Bounded Slack Events API intake for Director.

This module accepts only pointer metadata from a single trusted Slack source.
It deliberately does not inspect, persist, or log message text.  Message bodies
are fetched separately as untrusted evidence after durable intake.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
import logging
from typing import Any, Protocol

from .inbox import InboundPointer


@dataclass(frozen=True)
class SlackAllowlist:
    """The one Slack source this receiver is allowed to ingest."""

    team_id: str
    channel_id: str
    owner_user_id: str
    workspace_domain: str | None = None

    def __post_init__(self) -> None:
        for name, value in (
            ("team_id", self.team_id),
            ("channel_id", self.channel_id),
            ("owner_user_id", self.owner_user_id),
        ):
            if not isinstance(value, str) or not value:
                raise ValueError(f"{name} must be a non-empty string")
        if self.workspace_domain is not None and (
            not isinstance(self.workspace_domain, str) or not self.workspace_domain
        ):
            raise ValueError("workspace_domain must be a non-empty string when provided")


class PointerStore(Protocol):
    def ingest(self, pointer: InboundPointer) -> object: ...


class InteractionHandler(Protocol):
    """Durably handles one trusted Slack block interaction."""

    def handle_interaction(self, payload: Mapping[str, Any]) -> object: ...


class SlackSourceError(RuntimeError):
    """A source fetch could not be tied to the trusted stored pointer."""


@dataclass(frozen=True)
class SlackSourceEvidence:
    """Untrusted Slack API response material for one stored pointer.

    ``message`` and ``thread`` can contain message text and files.  Callers must
    treat them as untrusted evidence and must not log them indiscriminately.
    """

    message: Mapping[str, Any]
    thread: tuple[Mapping[str, Any], ...]


@dataclass(frozen=True)
class SocketModeAck:
    """Small test-friendly representation of a Socket Mode acknowledgement."""

    envelope_id: str


def normalize_event(payload: Mapping[str, Any], allowlist: SlackAllowlist) -> InboundPointer | None:
    """Convert one Events API payload into a safe source pointer, or reject it.

    The returned ``event_ts`` is a source revision: original message timestamp
    for creates, and Slack's ``edited.ts`` for edits.  Delivery timestamps are
    intentionally never used for create events, so replaying a recovery scan
    does not manufacture a revision.
    """

    if _string(payload.get("team_id")) != allowlist.team_id:
        return None
    event_id = _string(payload.get("event_id"))
    event = _mapping(payload.get("event"))
    if not event_id or event is None or event.get("type") != "message":
        return None
    if _string(event.get("channel")) != allowlist.channel_id:
        return None

    subtype = _string(event.get("subtype"))
    if subtype in (None, "file_share", "thread_broadcast"):
        return _create_pointer(event_id, event, subtype or "message", allowlist)
    if subtype == "message_changed":
        return _changed_pointer(event_id, event, allowlist)
    if subtype == "message_deleted":
        return _deleted_pointer(event_id, event, allowlist)
    return None


def make_socket_mode_listener(
    store: PointerStore,
    allowlist: SlackAllowlist,
    *,
    logger: logging.Logger | None = None,
    response_factory: Callable[[str], object] | None = None,
    interaction_handler: InteractionHandler | None = None,
    intake_ack_notifier: Callable[[], None] | None = None,
) -> Callable[[object, object], None]:
    """Build an official Slack Socket Mode listener with durable-before-ack order.

    Invalid or out-of-scope events are acknowledged so Slack does not retry
    them.  If durable storage raises, the event is deliberately unacknowledged.
    """

    log = logger or logging.getLogger(__name__)
    factory = response_factory or _socket_mode_response

    def listener(client: object, request: object) -> None:
        payload = _mapping(getattr(request, "payload", None))
        envelope_id = _string(getattr(request, "envelope_id", None))
        if payload is None or not envelope_id:
            return
        try:
            if getattr(request, "type", None) == "events_api":
                pointer = normalize_event(payload, allowlist)
                if pointer is not None:
                    store.ingest(pointer)
                    # The queue row was committed by InboxStore.ingest().  A
                    # wake is intentionally tiny and best-effort: the worker
                    # recovers durable rows even if this signal is missed.
                    if intake_ack_notifier is not None:
                        try:
                            intake_ack_notifier()
                        except Exception as error:
                            log.error("Slack intake reaction wake failed (%s)", type(error).__name__)
            elif getattr(request, "type", None) == "interactive" and interaction_handler is not None:
                interaction_handler.handle_interaction(payload)
            else:
                return
            _send_ack(client, factory(envelope_id))
        except Exception as error:
            # Do not include exception text, a request, or a payload here: all
            # may carry private Slack content or credentials.
            log.error(
                "Slack event intake failed; leaving event unacknowledged (%s)",
                type(error).__name__,
            )

    return listener


def create_socket_mode_client(
    app_token: str,
    bot_token: str,
    store: PointerStore,
    allowlist: SlackAllowlist,
    *,
    logger: logging.Logger | None = None,
    interaction_handler: InteractionHandler | None = None,
) -> object:
    """Create a SocketModeClient lazily, so receiving tests need no SDK install."""

    from slack_sdk import WebClient
    from slack_sdk.socket_mode import SocketModeClient

    client = SocketModeClient(app_token=app_token, web_client=WebClient(token=bot_token))
    client.socket_mode_request_listeners.append(
        make_socket_mode_listener(store, allowlist, logger=logger, interaction_handler=interaction_handler)
    )
    return client


def fetch_source_evidence(
    web_client: object,
    pointer: InboundPointer,
    allowlist: SlackAllowlist,
    *,
    page_size: int = 200,
) -> SlackSourceEvidence:
    """Fetch one stored source message and its thread, verifying trusted identity.

    No Slack mutation is made.  In particular, this function does not add a
    reaction or otherwise mark the message read.
    """

    if page_size < 1 or page_size > 1000:
        raise ValueError("page_size must be between 1 and 1000")
    if (
        pointer.source_team_id != allowlist.team_id
        or pointer.source_channel_id != allowlist.channel_id
    ):
        raise SlackSourceError("source pointer is outside the trusted allowlist")

    root_ts = pointer.thread_ts or pointer.source_ts
    cursor: str | None = None
    seen_cursors: set[str] = set()
    thread: list[Mapping[str, Any]] = []
    found: Mapping[str, Any] | None = None

    while True:
        kwargs: dict[str, Any] = {"channel": allowlist.channel_id, "ts": root_ts, "limit": page_size}
        if cursor:
            kwargs["cursor"] = cursor
        response = _mapping(getattr(web_client, "conversations_replies")(**kwargs))
        if response is None or response.get("ok") is False:
            raise SlackSourceError("Slack source fetch failed")
        _verify_response_channel(response, allowlist.channel_id)
        messages = response.get("messages")
        if not isinstance(messages, list):
            raise SlackSourceError("Slack source fetch did not return messages")
        for item in messages:
            message = _mapping(item)
            if message is None:
                continue
            thread.append(message)
            if _string(message.get("ts")) == pointer.source_ts:
                found = message

        next_cursor = _next_cursor(response)
        if response.get("has_more") is True and not next_cursor:
            raise SlackSourceError("Slack source fetch ended before the full thread was returned")
        if not next_cursor:
            break
        if next_cursor in seen_cursors:
            raise SlackSourceError("Slack source fetch repeated a cursor")
        seen_cursors.add(next_cursor)
        cursor = next_cursor

    if found is None:
        raise SlackSourceError("stored source timestamp was not returned by Slack")
    if _string(found.get("user")) != allowlist.owner_user_id or _is_bot_message(found):
        raise SlackSourceError("Slack source author is outside the trusted allowlist")
    found_root_ts = _string(found.get("thread_ts")) or _string(found.get("ts"))
    if found_root_ts != root_ts:
        raise SlackSourceError("Slack source message is not in the trusted thread")
    return SlackSourceEvidence(message=found, thread=tuple(thread))


def _create_pointer(
    event_id: str, event: Mapping[str, Any], event_type: str, allowlist: SlackAllowlist
) -> InboundPointer | None:
    source_ts = _string(event.get("ts"))
    if not source_ts or _string(event.get("user")) != allowlist.owner_user_id or _is_bot_message(event):
        return None
    return InboundPointer(
        event_id=event_id,
        source_team_id=allowlist.team_id,
        source_channel_id=allowlist.channel_id,
        source_ts=source_ts,
        event_ts=source_ts,
        thread_ts=_canonical_thread_ts(event.get("thread_ts"), source_ts),
        event_type=event_type,
    )


def _changed_pointer(event_id: str, event: Mapping[str, Any], allowlist: SlackAllowlist) -> InboundPointer | None:
    message = _mapping(event.get("message"))
    if message is None or _string(message.get("user")) != allowlist.owner_user_id or _is_bot_message(message):
        return None
    source_ts = _string(message.get("ts"))
    edited = _mapping(message.get("edited"))
    revision_ts = _string(edited.get("ts")) if edited is not None else None
    if not source_ts:
        return None
    return InboundPointer(
        event_id=event_id,
        source_team_id=allowlist.team_id,
        source_channel_id=allowlist.channel_id,
        source_ts=source_ts,
        event_ts=revision_ts or source_ts,
        thread_ts=_canonical_thread_ts(message.get("thread_ts"), source_ts),
        event_type="message_changed",
    )


def _deleted_pointer(event_id: str, event: Mapping[str, Any], allowlist: SlackAllowlist) -> InboundPointer | None:
    previous = _mapping(event.get("previous_message"))
    if previous is None or _string(previous.get("user")) != allowlist.owner_user_id or _is_bot_message(previous):
        return None
    source_ts = _string(previous.get("ts")) or _string(event.get("deleted_ts"))
    if not source_ts:
        return None
    # Slack has no stable deleted-revision field. event_ts is generated at the
    # deletion and is the only revision Slack provides for this event.
    revision_ts = _string(event.get("event_ts")) or source_ts
    return InboundPointer(
        event_id=event_id,
        source_team_id=allowlist.team_id,
        source_channel_id=allowlist.channel_id,
        source_ts=source_ts,
        event_ts=revision_ts,
        thread_ts=_canonical_thread_ts(previous.get("thread_ts"), source_ts),
        event_type="message_deleted",
    )


def _is_bot_message(message: Mapping[str, Any]) -> bool:
    return bool(message.get("bot_id")) or _string(message.get("subtype")) == "bot_message"


def _verify_response_channel(response: Mapping[str, Any], expected_channel: str) -> None:
    channel = response.get("channel")
    if channel is None:
        return
    channel_id = _string(channel)
    if channel_id is None:
        channel_mapping = _mapping(channel)
        channel_id = _string(channel_mapping.get("id")) if channel_mapping is not None else None
    if channel_id != expected_channel:
        raise SlackSourceError("Slack source response channel does not match allowlist")


def _next_cursor(response: Mapping[str, Any]) -> str | None:
    metadata = _mapping(response.get("response_metadata"))
    return _string(metadata.get("next_cursor")) if metadata is not None else None


def _send_ack(client: object, response: object) -> None:
    sender = getattr(client, "send_socket_mode_response", None)
    if not callable(sender):
        raise TypeError("Socket Mode client does not support acknowledgements")
    sender(response)


def _socket_mode_response(envelope_id: str) -> object:
    from slack_sdk.socket_mode.response import SocketModeResponse

    return SocketModeResponse(envelope_id=envelope_id)


def _mapping(value: object) -> Mapping[str, Any] | None:
    if isinstance(value, Mapping):
        return value
    # slack_sdk.WebClient returns a SlackResponse, which exposes its JSON as
    # ``data`` rather than registering as collections.abc.Mapping.
    data = getattr(value, "data", None)
    return data if isinstance(data, Mapping) else None


def _string(value: object) -> str | None:
    return value if isinstance(value, str) and value else None


def _canonical_thread_ts(value: object, source_ts: str) -> str | None:
    """Represent a top-level Slack message consistently before and after replies."""

    thread_ts = _string(value)
    return None if thread_ts == source_ts else thread_ts
