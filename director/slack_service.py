"""Bounded Slack service operations for Director.

The receiver only stores source pointers.  This module contains the separately
authorized operations that fetch source evidence, add a read reaction after a
manager command, recover missed owner messages, and send Director messages
through a durable, conservative outbox.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import hashlib
import logging
from pathlib import Path
import sqlite3
import threading
import time
import uuid
from typing import Any, Callable, Protocol
from urllib.parse import parse_qs, urlparse

from .inbox import InboundPointer
from .responsibilities import ResponsibilityError, apply_card_action, initialize_schema, link_attention_card, resurface_card_attention
from .slack_transport import (
    SlackAllowlist,
    SlackSourceEvidence,
    fetch_source_evidence,
    normalize_event,
)


RECOVERY_OLDEST = "1788835027"


class SlackServiceError(RuntimeError):
    """A bounded Slack service operation could not be completed safely."""


class InboxServiceStore(Protocol):
    def ingest(self, pointer: InboundPointer) -> object: ...

    def get_message(self, message_id: int) -> object: ...

    def mark_read_if_revision(self, message_id: int, revision: int, *, now: float | None = None) -> bool: ...

    def list_source_threads(self) -> tuple[object, ...]: ...


@dataclass(frozen=True)
class OutboxEntry:
    idempotency_key: str
    client_msg_id: str
    channel_id: str
    thread_ts: str | None
    text: str
    state: str
    slack_ts: str | None


@dataclass(frozen=True)
class OutgoingResult:
    idempotency_key: str
    client_msg_id: str
    state: str
    slack_ts: str | None


@dataclass(frozen=True)
class InboxCard:
    """One durable, user-actionable Director inbox card."""

    id: str
    idempotency_key: str
    client_msg_id: str
    channel_id: str
    conversation_url: str
    title: str
    body: str
    state: str
    revisit_at: float | None
    version: int
    delivery_state: str
    slack_ts: str | None
    resolution_state: str


@dataclass(frozen=True)
class InboxCardAction:
    """Result of one accepted interaction; retries return the same result."""

    card: InboxCard
    changed: bool


@dataclass(frozen=True)
class FetchedOwnerSource:
    """Verified source evidence paired with the current durable message revision."""

    evidence: SlackSourceEvidence
    message: object
    source_updated: bool


class OutgoingAuthorizationError(SlackServiceError):
    """A caller's source authority changed before Slack dispatch."""


class SlackService:
    """Slack operations bounded to a single allowlisted owner and channel."""

    def __init__(
        self,
        web_client: object,
        store: InboxServiceStore,
        allowlist: SlackAllowlist,
        outbox_path: str | Path,
        *,
        recovery_oldest: str = RECOVERY_OLDEST,
        logger: logging.Logger | None = None,
    ) -> None:
        if not _slack_ts(recovery_oldest):
            raise ValueError("recovery_oldest must be a Slack timestamp")
        self._web_client = web_client
        self._store = store
        self.allowlist = allowlist
        self.recovery_oldest = recovery_oldest
        self._log = logger or logging.getLogger(__name__)
        self._lock = threading.RLock()
        # This is intentionally a distinct connection from InboxStore, even if
        # both stores share one SQLite database file.
        self._connection = sqlite3.connect(str(outbox_path), isolation_level=None, check_same_thread=False)
        self._connection.row_factory = sqlite3.Row
        self._connection.execute("PRAGMA busy_timeout = 5000")
        self._initialize_outbox()

    def close(self) -> None:
        with self._lock:
            self._connection.close()

    def __enter__(self) -> "SlackService":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def fetch_owner_source_evidence(self, message: object) -> FetchedOwnerSource:
        """Fetch verified evidence and durably ingest a newer source edit first.

        The response carries the durable message after that ingest.  A caller
        that was acting on a previously observed revision must compare it before
        doing an external mutation such as a read reaction.
        """

        evidence = fetch_source_evidence(self._web_client, self._pointer_for_message(message), self.allowlist)
        current_event_ts = _canonical_source_revision(evidence.message)
        stored_event_ts = _slack_ts(getattr(message, "event_ts", None))
        if current_event_ts is None or stored_event_ts is None:
            raise SlackServiceError("source evidence has no usable revision")
        if current_event_ts == stored_event_ts:
            return FetchedOwnerSource(evidence=evidence, message=message, source_updated=False)

        pointer = self._recovery_pointer(evidence.message)
        if pointer is None:
            raise SlackServiceError("source evidence cannot be normalized")
        result = self._store.ingest(pointer)
        refreshed = getattr(result, "message", None)
        if refreshed is None:
            refreshed = self._store.get_message(getattr(message, "id"))
        return FetchedOwnerSource(evidence=evidence, message=refreshed, source_updated=True)

    def mark_read_after_manager_command(self, message_id: int, *, expected_revision: int | None = None) -> bool:
        """React to a verified source, then atomically record that exact revision read.

        Calling this method is the explicit manager-command boundary.  No
        recovery or outbound method invokes it implicitly.
        """

        message = self._store.get_message(message_id)
        revision = _positive_int(getattr(message, "revision", None))
        if revision is None:
            raise SlackServiceError("stored message has no usable revision")
        if expected_revision is not None and expected_revision != revision:
            return False
        fetched = self.fetch_owner_source_evidence(message)
        message = fetched.message
        revision = _positive_int(getattr(message, "revision", None))
        if revision is None:
            raise SlackServiceError("refreshed stored message has no usable revision")
        if expected_revision is not None and expected_revision != revision:
            return False
        pointer = self._pointer_for_message(message)

        # Re-verify after the source read.  An edit during the fetch must not
        # receive a reaction or receipt intended for the older revision.
        current = self._store.get_message(message_id)
        if not _same_message_revision(current, message):
            return False

        try:
            response = _response_data(
                getattr(self._web_client, "reactions_add")(
                    channel=pointer.source_channel_id,
                    timestamp=pointer.source_ts,
                    name="white_check_mark",
                )
            )
            if response is not None and response.get("ok") is False:
                raise SlackServiceError("Slack rejected the read reaction")
        except Exception as error:
            if not _already_reacted(error):
                self._log_sdk_failure("Slack read reaction failed", error)
                raise SlackServiceError("Slack read reaction failed") from None
        return bool(self._store.mark_read_if_revision(message_id, revision))

    def send_outgoing(
        self,
        text: str,
        *,
        idempotency_key: str,
        thread_ts: str | None = None,
        authorize: Callable[[], bool] | None = None,
    ) -> OutgoingResult:
        """Durably prepare and send once; never auto-retry an uncertain request.

        A process crash or SDK exception around ``chat_postMessage`` is treated
        as uncertain.  Later calls reconcile by ``client_msg_id`` and otherwise
        remain pending for an explicit operational decision, rather than risk a
        duplicate Slack message.
        """

        if not isinstance(text, str) or not text:
            raise ValueError("text must be a non-empty string")
        if not isinstance(idempotency_key, str) or not idempotency_key:
            raise ValueError("idempotency_key must be a non-empty string")
        if thread_ts is not None and not _slack_ts(thread_ts):
            raise ValueError("thread_ts must be a Slack timestamp")

        if authorize is not None and not authorize():
            raise OutgoingAuthorizationError("outgoing authority is no longer current")
        client_msg_id = _client_msg_id(self.allowlist, idempotency_key, thread_ts)
        entry, created = self._prepare_outbox(idempotency_key, client_msg_id, text, thread_ts)
        if not created:
            if entry.text != text or entry.thread_ts != thread_ts:
                raise SlackServiceError("idempotency key was reused for a different outgoing message")
            if entry.state == "sent":
                return _outgoing_result(entry)
            self._set_outbox_state(entry.idempotency_key, "pending_uncertain", entry.slack_ts)
            return self.reconcile_outgoing(entry.idempotency_key)

        try:
            # A source edit or a stale ACP turn can win after durable outbox
            # preparation. Never reserve the external send without a second
            # caller-owned authority check.
            if authorize is not None and not authorize():
                raise OutgoingAuthorizationError("outgoing authority changed before dispatch")
            kwargs: dict[str, Any] = {
                "channel": self.allowlist.channel_id,
                "text": text,
                "client_msg_id": client_msg_id,
            }
            if thread_ts is not None:
                kwargs["thread_ts"] = thread_ts
            response = _response_data(getattr(self._web_client, "chat_postMessage")(**kwargs))
            slack_ts = _successful_send_ts(response)
            if slack_ts is None:
                raise SlackServiceError("Slack send response was not a confirmed message")
            self._set_outbox_state(idempotency_key, "sent", slack_ts)
            return _outgoing_result(self._get_outbox(idempotency_key))
        except OutgoingAuthorizationError:
            # The outbox row remains prepared for diagnostic/reconciliation;
            # it is never converted into a retryable send after authority
            # changed between durable preparation and dispatch.
            raise
        except Exception as error:
            self._set_outbox_state(idempotency_key, "pending_uncertain", None)
            self._log_sdk_failure("Slack outgoing send is uncertain", error)
            return self.reconcile_outgoing(idempotency_key)

    def _dispatch_prepared_outgoing(self, entry: OutboxEntry) -> OutgoingResult:
        """Make the one network attempt for a durably prepared outbox row."""

        try:
            kwargs: dict[str, Any] = {
                "channel": self.allowlist.channel_id,
                "text": entry.text,
                "client_msg_id": entry.client_msg_id,
            }
            if entry.thread_ts is not None:
                kwargs["thread_ts"] = entry.thread_ts
            response = _response_data(getattr(self._web_client, "chat_postMessage")(**kwargs))
            slack_ts = _successful_send_ts(response)
            if slack_ts is None:
                raise SlackServiceError("Slack send response was not a confirmed message")
            self._set_outbox_state(entry.idempotency_key, "sent", slack_ts)
            return _outgoing_result(self._get_outbox(entry.idempotency_key))
        except Exception as error:
            self._set_outbox_state(entry.idempotency_key, "pending_uncertain", None)
            self._log_sdk_failure("Slack outgoing send is uncertain", error)
            return self.reconcile_outgoing(entry.idempotency_key)

    def create_inbox_card(
        self,
        title: str,
        body: str,
        conversation_url: str,
        *,
        idempotency_key: str,
        responsibility_id: str | None = None,
        responsibility_version: int | None = None,
    ) -> InboxCard:
        """Create one top-level, durable personal inbox card.

        A reused key never creates another Slack card.  As with the existing
        outbox, an uncertain first request is reconciled and is never blindly
        sent again.
        """

        _validate_card_input(title, body, conversation_url, idempotency_key, self.allowlist)
        card, created = self._prepare_inbox_card(title, body, conversation_url, idempotency_key, responsibility_id, responsibility_version)
        if not created:
            if (card.title, card.body, card.conversation_url) != (title, body, conversation_url):
                raise SlackServiceError("idempotency key was reused for a different inbox card")
            if card.delivery_state == "sent":
                return card
            self._set_card_delivery(card.id, "pending_uncertain", card.slack_ts)
            return self.reconcile_inbox_card(idempotency_key)

        try:
            response = _response_data(
                getattr(self._web_client, "chat_postMessage")(
                    channel=self.allowlist.channel_id,
                    text=_card_fallback_text(card),
                    blocks=_card_blocks(card),
                    client_msg_id=card.client_msg_id,
                )
            )
            slack_ts = _successful_send_ts(response)
            if slack_ts is None:
                raise SlackServiceError("Slack send response was not a confirmed inbox card")
            self._set_card_delivery(card.id, "sent", slack_ts)
            return self._get_inbox_card(card.id)
        except Exception as error:
            self._set_card_delivery(card.id, "pending_uncertain", None)
            self._log_sdk_failure("Slack inbox card send is uncertain", error)
            return self.reconcile_inbox_card(idempotency_key)

    def publish_responsibility_result(
        self,
        responsibility_id: str,
        execution_fence: str,
        text: str,
        *,
        idempotency_key: str,
        authorize: Callable[[], bool] | None = None,
    ) -> OutgoingResult | None:
        """Fence a result and send it to the responsibility's current thread.

        The authorization and a prepared outbox row commit before Slack is
        called. A second short transaction records dispatch as started only
        while the fence remains valid. A Later or Drop can win before that
        reservation; after it, an already-started external dispatch cannot be
        retracted, but crash recovery reconciles rather than resends it.
        """

        if not responsibility_id or not execution_fence or not idempotency_key or not text:
            raise ValueError("responsibility id, fence, text, and idempotency key are required")
        now = time.time()
        with self._lock:
            self._connection.execute("BEGIN IMMEDIATE")
            try:
                if authorize is not None and not authorize():
                    raise OutgoingAuthorizationError("outgoing authority is no longer current")
                responsibility = self._connection.execute(
                    """
                    SELECT current_thread FROM responsibilities
                    WHERE id = ? AND state = 'claimed' AND execution_fence = ? AND claim_expires_at > ?
                    """,
                    (responsibility_id, execution_fence, now),
                ).fetchone()
                if responsibility is None:
                    self._connection.execute("ROLLBACK")
                    return None
                thread_ts = _slack_ts(responsibility["current_thread"])
                if thread_ts is None:
                    raise SlackServiceError("responsibility has no usable current Slack thread")
                existing = self._connection.execute(
                    "SELECT execution_fence, summary, state FROM responsibility_publications WHERE responsibility_id = ? AND idempotency_key = ?",
                    (responsibility_id, idempotency_key),
                ).fetchone()
                if existing is not None and (existing["execution_fence"] != execution_fence or existing["summary"] != text):
                    raise SlackServiceError("publication key was reused for a different result")
                if existing is None:
                    self._connection.execute(
                        "INSERT INTO responsibility_publications (responsibility_id, idempotency_key, execution_fence, summary, created_at) VALUES (?, ?, ?, ?, ?)",
                        (responsibility_id, idempotency_key, execution_fence, text, now),
                    )
                client_msg_id = _client_msg_id(self.allowlist, idempotency_key, thread_ts)
                entry, created = self._prepare_outbox(idempotency_key, client_msg_id, text, thread_ts)
                if (entry.text, entry.thread_ts, entry.client_msg_id) != (text, thread_ts, client_msg_id):
                    raise SlackServiceError("idempotency key was reused for a different outgoing result")
                if not created and existing is None:
                    raise SlackServiceError("idempotency key is already bound outside this responsibility")
                self._connection.execute("COMMIT")
            except Exception:
                if self._connection.in_transaction:
                    self._connection.execute("ROLLBACK")
                raise
        if not created and existing["state"] != "prepared":
            return self.reconcile_outgoing(idempotency_key)

        with self._lock:
            self._connection.execute("BEGIN IMMEDIATE")
            try:
                if authorize is not None and not authorize():
                    raise OutgoingAuthorizationError("outgoing authority changed before dispatch")
                still_allowed = self._connection.execute(
                    "SELECT 1 FROM responsibilities WHERE id = ? AND state = 'claimed' AND execution_fence = ? AND claim_expires_at > ?",
                    (responsibility_id, execution_fence, time.time()),
                ).fetchone()
                if still_allowed is None:
                    self._connection.execute("ROLLBACK")
                    return None
                reserved = self._connection.execute(
                    """
                    UPDATE responsibility_publications
                    SET state = 'dispatching', dispatch_started_at = ?
                    WHERE responsibility_id = ? AND idempotency_key = ? AND state = 'prepared'
                    """,
                    (time.time(), responsibility_id, idempotency_key),
                ).rowcount
                if reserved != 1:
                    self._connection.execute("ROLLBACK")
                    return self.reconcile_outgoing(idempotency_key)
                self._connection.execute("COMMIT")
            except Exception:
                if self._connection.in_transaction:
                    self._connection.execute("ROLLBACK")
                raise
        return self._dispatch_prepared_outgoing(entry)

    def reconcile_inbox_card(self, idempotency_key: str) -> InboxCard:
        """Reconcile an uncertain card by client id without sending another."""

        card = self._get_inbox_card_by_key(idempotency_key)
        if card.delivery_state == "sent":
            return card
        if card.delivery_state == "prepared":
            self._set_card_delivery(card.id, "pending_uncertain", None)
            card = self._get_inbox_card(card.id)
        try:
            found = self._find_client_message(
                "conversations_history",
                OutboxEntry(card.idempotency_key, card.client_msg_id, card.channel_id, None, "", "pending_uncertain", None),
                channel=card.channel_id,
                limit=200,
            )
        except Exception as error:
            self._log_sdk_failure("Slack inbox card reconciliation failed", error)
            found = None
        if found is not None:
            self._set_card_delivery(card.id, "sent", found)
        return self._get_inbox_card(card.id)

    def repair_inbox_card_link(self, idempotency_key: str, conversation_url: str) -> InboxCard:
        """Replace a sent card's conversation link without recreating its card or state."""

        with self._lock:
            card = self._get_inbox_card_by_key(idempotency_key)
            _validate_card_input(card.title, card.body, conversation_url, idempotency_key, self.allowlist)
            if card.delivery_state != "sent":
                raise SlackServiceError("inbox card has not been confirmed in Slack")
            if card.conversation_url != conversation_url:
                self._connection.execute(
                    """
                    UPDATE slack_inbox_cards
                    SET conversation_url = ?, render_state = 'pending', updated_at = ?
                    WHERE id = ?
                    """,
                    (conversation_url, time.time(), card.id),
                )
        self.reconcile_pending_inbox_card_renders()
        return self._get_inbox_card(card.id)

    def handle_interaction(self, payload: Mapping[str, Any]) -> InboxCardAction | None:
        """Apply one trusted card action, then update the existing Slack card.

        ``None`` means that the action was out of scope or stale.  Such
        payloads are acknowledged by Socket Mode but have no durable effect.
        Storage failures raise so Socket Mode deliberately retries. Slack
        rendering happens afterward in the receiver loop and stays pending if
        that later update fails.
        """

        parsed = _parse_card_interaction(payload, self.allowlist)
        if parsed is None:
            return None
        action_id, action_ts, card_id, expected_version, defer_seconds, message_ts = parsed
        if action_id == "director_open_conversation":
            # Open is a URL action in the card.  Retaining this no-op makes a
            # forged or historical block action harmless as well.
            return None

        with self._lock:
            self._connection.execute("BEGIN IMMEDIATE")
            try:
                receipt = self._connection.execute(
                    "SELECT card_id, action_id, changed FROM slack_inbox_card_actions WHERE action_ts = ?", (action_ts,)
                ).fetchone()
                if receipt is not None:
                    if receipt["card_id"] != card_id or receipt["action_id"] != action_id:
                        self._connection.execute("ROLLBACK")
                        return None
                    result = InboxCardAction(card=self._get_inbox_card(card_id), changed=bool(receipt["changed"]))
                    self._connection.execute("COMMIT")
                    return result

                card = self._get_inbox_card(card_id)
                if (
                    card.channel_id != self.allowlist.channel_id
                    or card.delivery_state != "sent"
                    or card.slack_ts != message_ts
                    or card.state != "pending"
                    or card.version != expected_version
                ):
                    self._connection.execute("ROLLBACK")
                    return None
                state = "deferred" if action_id == "director_later" else "dropped"
                revisit_at = time.time() + defer_seconds if defer_seconds is not None else None
                if state == "deferred" and revisit_at is None:
                    self._connection.execute("ROLLBACK")
                    return None
                now = time.time()
                updated_count = self._connection.execute(
                    """
                    UPDATE slack_inbox_cards
                    SET state = ?, revisit_at = ?, version = version + 1, render_state = 'pending',
                        resurface_nudge_key = NULL, resurface_nudge_sent_at = NULL, updated_at = ?
                    WHERE id = ? AND state = 'pending' AND version = ?
                    """,
                    (state, revisit_at, now, card.id, expected_version),
                ).rowcount
                if updated_count != 1:
                    self._connection.execute("ROLLBACK")
                    return None
                apply_card_action(self._connection, card.id, action_id, revisit_at, now)
                self._connection.execute(
                    """
                    INSERT INTO slack_inbox_card_actions (action_ts, card_id, action_id, changed, created_at)
                    VALUES (?, ?, ?, 1, ?)
                    """,
                    (action_ts, card.id, action_id, now),
                )
                result = InboxCardAction(card=self._get_inbox_card(card.id), changed=True)
                self._connection.execute("COMMIT")
                return result
            except Exception:
                if self._connection.in_transaction:
                    self._connection.execute("ROLLBACK")
                raise

    def ensure_waiting_attention_cards(self, *, now: float | None = None) -> int:
        """Repair missing attention UI without a model polling turn.

        Give the manager time to post a tailored card first. Version fencing
        prevents a concurrent continuation or cancellation from being undone.
        """
        current_time = time.time() if now is None else now
        with self._lock:
            rows = self._connection.execute(
                "SELECT * FROM responsibilities WHERE state = 'waiting_input' "
                "AND attention_card_id IS NULL AND updated_at <= ?",
                (current_time - 30,),
            ).fetchall()
        repaired = 0
        for row in rows:
            root = _slack_ts(row["current_thread"])
            if root is None or not self.allowlist.workspace_domain:
                continue
            url = f"https://{self.allowlist.workspace_domain}/archives/{self.allowlist.channel_id}/p{root.replace('.', '')}?thread_ts={root}&cid={self.allowlist.channel_id}"
            try:
                self.create_inbox_card(
                    row["outcome"][:100], row["next_action"][:3000], url,
                    idempotency_key=f"attention-repair:{row['id']}:v{row['version']}",
                    responsibility_id=row["id"], responsibility_version=row["version"],
                )
            except ResponsibilityError:
                continue  # The manager or owner changed this responsibility.
            repaired += 1
        return repaired

    def reconcile_pending_inbox_card_renders(self) -> int:
        """Render durable action results outside Socket Mode's acknowledgement path."""

        with self._lock:
            rows = self._connection.execute(
                "SELECT id FROM slack_inbox_cards WHERE delivery_state = 'sent' AND render_state = 'pending'"
            ).fetchall()
        rendered = 0
        for row in rows:
            card = self._get_inbox_card(str(row["id"]))
            try:
                self._update_inbox_card(card)
            except SlackServiceError:
                continue
            with self._lock:
                acknowledged = self._connection.execute(
                    """
                    UPDATE slack_inbox_cards SET render_state = 'rendered', updated_at = ?
                    WHERE id = ? AND version = ? AND conversation_url = ? AND render_state = 'pending'
                    """,
                    (time.time(), card.id, card.version, card.conversation_url),
                ).rowcount
            rendered += acknowledged
        return rendered

    def resurface_due_inbox_cards(self, *, now: float | None = None) -> int:
        """Restore each due deferred card once and durably prepare its nudge.

        The same card is made actionable again.  The nudge key is written in
        the transaction with the state transition, so a receiver restart can
        finish delivery without creating a second reminder.
        """

        current_time = time.time() if now is None else now
        if not isinstance(current_time, (float, int)):
            raise ValueError("now must be Unix seconds")
        with self._lock:
            self._connection.execute("BEGIN IMMEDIATE")
            try:
                rows = self._connection.execute(
                    """
                    SELECT id, resurface_cycle
                    FROM slack_inbox_cards
                    WHERE state = 'deferred' AND revisit_at <= ? AND delivery_state = 'sent'
                    """,
                    (float(current_time),),
                ).fetchall()
                for row in rows:
                    card_id = str(row["id"])
                    cycle = int(row["resurface_cycle"]) + 1
                    nudge_key = _resurface_nudge_key(card_id, cycle)
                    updated = self._connection.execute(
                        """
                        UPDATE slack_inbox_cards
                        SET state = 'pending', revisit_at = NULL, version = version + 1,
                            render_state = 'pending', resurface_cycle = ?,
                            resurface_nudge_key = ?, resurface_nudge_sent_at = NULL,
                            updated_at = ?
                        WHERE id = ? AND state = 'deferred' AND revisit_at <= ?
                        """,
                        (cycle, nudge_key, float(current_time), card_id, float(current_time)),
                    ).rowcount
                    if updated != 1:
                        raise SlackServiceError("due inbox card changed during resurfacing")
                    resurface_card_attention(self._connection, card_id, float(current_time))
                self._connection.execute("COMMIT")
            except Exception:
                if self._connection.in_transaction:
                    self._connection.execute("ROLLBACK")
                raise
        return len(rows)

    def deliver_pending_inbox_card_nudges(self) -> int:
        """Send one top-level nudge for each rendered resurfaced card.

        ``send_outgoing`` owns uncertain-send reconciliation.  Keeping the
        stable key on the card means a crash after Slack accepts the nudge but
        before this method records it cannot create a duplicate.
        """

        with self._lock:
            rows = self._connection.execute(
                """
                SELECT id FROM slack_inbox_cards
                WHERE state = 'pending' AND delivery_state = 'sent'
                  AND render_state = 'rendered'
                  AND resurface_nudge_key IS NOT NULL
                  AND resurface_nudge_sent_at IS NULL
                """
            ).fetchall()
        delivered = 0
        for row in rows:
            with self._lock:
                card = self._get_inbox_card(str(row["id"]))
                if (
                    card.state != "pending"
                    or card.delivery_state != "sent"
                    or self._card_nudge_sent(card.id)
                    or self._card_nudge_key(card.id) is None
                ):
                    continue
                nudge_key = self._card_nudge_key(card.id)
            result = self.send_outgoing(_resurface_nudge_text(card, self.allowlist), idempotency_key=nudge_key)
            if result.state != "sent":
                continue
            with self._lock:
                updated = self._connection.execute(
                    """
                    UPDATE slack_inbox_cards
                    SET resurface_nudge_sent_at = ?, updated_at = ?
                    WHERE id = ? AND state = 'pending' AND resurface_nudge_key = ?
                      AND resurface_nudge_sent_at IS NULL
                    """,
                    (time.time(), time.time(), card.id, nudge_key),
                ).rowcount
            if updated == 1:
                delivered += 1
        return delivered

    def test_defer_inbox_card(self, idempotency_key: str, due_at: float, *, now: float | None = None) -> InboxCard:
        """Set a clearly labelled synthetic card aside for a short live test.

        This explicit test-only operation cannot change ordinary
        responsibilities.  Production Later selections remain the sole way
        normal cards become deferred.
        """

        current_time = time.time() if now is None else now
        if not isinstance(due_at, (float, int)) or not isinstance(current_time, (float, int)):
            raise ValueError("test due time must be Unix seconds")
        if not str(idempotency_key).startswith("test-"):
            raise ValueError("test-only inbox card key must start with test-")
        if not float(current_time) < float(due_at) <= float(current_time) + 3600:
            raise ValueError("test-only inbox card due time must be within the next hour")
        with self._lock:
            card = self._get_inbox_card_by_key(idempotency_key)
            if not card.title.startswith("TEST:"):
                raise ValueError("test-only inbox card title must start with TEST:")
            if card.state != "pending":
                raise SlackServiceError("test-only inbox card must be pending")
            self._connection.execute(
                """
                UPDATE slack_inbox_cards
                SET state = 'deferred', revisit_at = ?, version = version + 1,
                    render_state = 'pending', resurface_nudge_key = NULL,
                    resurface_nudge_sent_at = NULL, updated_at = ?
                WHERE id = ? AND state = 'pending'
                """,
                (float(due_at), float(current_time), card.id),
            )
            apply_card_action(self._connection, card.id, "director_later", float(due_at), float(current_time))
            return self._get_inbox_card(card.id)

    def reconcile_outgoing(self, idempotency_key: str) -> OutgoingResult:
        """Look for a persisted client id without sending another Slack message."""

        entry = self._get_outbox(idempotency_key)
        if entry.state == "sent":
            return _outgoing_result(entry)
        if entry.state == "prepared":
            # A prepared row from an earlier process could have crossed the
            # network call before it crashed, so it is not safe to resend.
            self._set_outbox_state(idempotency_key, "pending_uncertain", None)
            entry = self._get_outbox(idempotency_key)
        try:
            found = self._find_sent_client_message(entry)
        except Exception as error:
            self._log_sdk_failure("Slack outgoing reconciliation failed", error)
            found = None
        if found is not None:
            self._set_outbox_state(idempotency_key, "sent", found)
        return _outgoing_result(self._get_outbox(idempotency_key))

    def recover_owner_messages(self) -> int:
        """Ingest owner messages from channel history and every tracked thread."""

        messages, roots = self._recover_channel_history()
        threads = getattr(self._store, "list_source_threads")()
        for source_thread in threads:
            if (
                getattr(source_thread, "source_team_id", None) == self.allowlist.team_id
                and getattr(source_thread, "source_channel_id", None) == self.allowlist.channel_id
                and _slack_ts(getattr(source_thread, "thread_ts", None))
            ):
                roots.add(str(getattr(source_thread, "thread_ts")))
        roots.update(self._outbox_thread_roots())
        for root_ts in sorted(roots):
            messages.extend(self._recover_thread(root_ts))

        ingested = 0
        seen_events: set[str] = set()
        for message in messages:
            pointer = self._recovery_pointer(message)
            if pointer is None or pointer.event_id in seen_events:
                continue
            seen_events.add(pointer.event_id)
            self._store.ingest(pointer)
            ingested += 1
        return ingested

    def _recover_channel_history(self) -> tuple[list[Mapping[str, Any]], set[str]]:
        cursor: str | None = None
        seen_cursors: set[str] = set()
        messages: list[Mapping[str, Any]] = []
        roots: set[str] = set()
        while True:
            kwargs: dict[str, Any] = {
                "channel": self.allowlist.channel_id,
                "oldest": self.recovery_oldest,
                "limit": 200,
            }
            if cursor:
                kwargs["cursor"] = cursor
            response = self._slack_call("conversations_history", **kwargs)
            self._verify_response_channel(response)
            page = response.get("messages")
            if not isinstance(page, list):
                raise SlackServiceError("Slack recovery history did not return messages")
            for raw in page:
                message = _mapping(raw)
                if message is None:
                    continue
                messages.append(message)
                # History is also our way to discover owner-created thread
                # roots.  Do not scan arbitrary other-user/bot threads here;
                # replies to those are covered once they are tracked locally.
                if self._recovery_pointer(message) is not None:
                    root_ts = _slack_ts(message.get("thread_ts")) or _slack_ts(message.get("ts"))
                    if root_ts:
                        roots.add(root_ts)
            cursor = _next_cursor(response)
            if response.get("has_more") is True and not cursor:
                raise SlackServiceError("Slack recovery history ended before all pages were returned")
            if not cursor:
                return messages, roots
            if cursor in seen_cursors:
                raise SlackServiceError("Slack recovery history repeated a cursor")
            seen_cursors.add(cursor)

    def _recover_thread(self, root_ts: str) -> list[Mapping[str, Any]]:
        cursor: str | None = None
        seen_cursors: set[str] = set()
        messages: list[Mapping[str, Any]] = []
        while True:
            kwargs: dict[str, Any] = {"channel": self.allowlist.channel_id, "ts": root_ts, "limit": 200}
            if cursor:
                kwargs["cursor"] = cursor
            response = self._slack_call("conversations_replies", **kwargs)
            self._verify_response_channel(response)
            page = response.get("messages")
            if not isinstance(page, list):
                raise SlackServiceError("Slack recovery thread did not return messages")
            for raw in page:
                message = _mapping(raw)
                if message is not None:
                    messages.append(message)
            cursor = _next_cursor(response)
            if response.get("has_more") is True and not cursor:
                raise SlackServiceError("Slack recovery thread ended before all pages were returned")
            if not cursor:
                return messages
            if cursor in seen_cursors:
                raise SlackServiceError("Slack recovery thread repeated a cursor")
            seen_cursors.add(cursor)

    def _recovery_pointer(self, message: Mapping[str, Any]) -> InboundPointer | None:
        source_ts = _slack_ts(message.get("ts"))
        if source_ts is None:
            return None
        edited = _mapping(message.get("edited"))
        revision_ts = _slack_ts(edited.get("ts")) if edited is not None else source_ts
        event: dict[str, Any] = dict(message)
        event["type"] = "message"
        event["channel"] = self.allowlist.channel_id
        if edited is not None:
            event = {"type": "message", "subtype": "message_changed", "channel": self.allowlist.channel_id, "message": event}
        event_type = "message_changed" if edited is not None else str(message.get("subtype") or "message")
        event_id = _recovery_event_id(self.allowlist, source_ts, revision_ts, event_type)
        return normalize_event(
            {"team_id": self.allowlist.team_id, "event_id": event_id, "event": event}, self.allowlist
        )

    def _pointer_for_message(self, message: object) -> InboundPointer:
        team_id = getattr(message, "source_team_id", None)
        channel_id = getattr(message, "source_channel_id", None)
        source_ts = getattr(message, "source_ts", None)
        event_ts = getattr(message, "event_ts", None)
        thread_ts = getattr(message, "thread_ts", None)
        if (
            team_id != self.allowlist.team_id
            or channel_id != self.allowlist.channel_id
            or not _slack_ts(source_ts)
            or not _slack_ts(event_ts)
            or (thread_ts is not None and not _slack_ts(thread_ts))
        ):
            raise SlackServiceError("stored message is outside the trusted Slack source")
        return InboundPointer(
            event_id=f"stored:{getattr(message, 'id', source_ts)}:{event_ts}",
            source_team_id=team_id,
            source_channel_id=channel_id,
            source_ts=source_ts,
            event_ts=event_ts,
            thread_ts=thread_ts,
            event_type=str(getattr(message, "event_type", "message")),
        )

    def _slack_call(self, method_name: str, **kwargs: Any) -> Mapping[str, Any]:
        try:
            response = _response_data(getattr(self._web_client, method_name)(**kwargs))
        except Exception as error:
            self._log_sdk_failure("Slack recovery request failed", error)
            raise SlackServiceError("Slack recovery request failed") from None
        if response is None or response.get("ok") is False:
            raise SlackServiceError("Slack recovery request failed")
        return response

    def _verify_response_channel(self, response: Mapping[str, Any]) -> None:
        channel = response.get("channel")
        if channel is None:
            return
        if isinstance(channel, str):
            value = channel
        else:
            channel_data = _mapping(channel)
            value = channel_data.get("id") if channel_data is not None else None
        if value != self.allowlist.channel_id:
            raise SlackServiceError("Slack recovery response channel does not match allowlist")

    def _prepare_outbox(
        self, idempotency_key: str, client_msg_id: str, text: str, thread_ts: str | None
    ) -> tuple[OutboxEntry, bool]:
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM slack_outbox WHERE idempotency_key = ?", (idempotency_key,)
            ).fetchone()
            if row is not None:
                return _outbox_entry(row), False
            now = time.time()
            self._connection.execute(
                """
                INSERT INTO slack_outbox (
                    idempotency_key, client_msg_id, channel_id, thread_ts, text,
                    state, slack_ts, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, 'prepared', NULL, ?, ?)
                """,
                (idempotency_key, client_msg_id, self.allowlist.channel_id, thread_ts, text, now, now),
            )
            return self._get_outbox(idempotency_key), True

    def _prepare_inbox_card(
        self, title: str, body: str, conversation_url: str, idempotency_key: str, responsibility_id: str | None, responsibility_version: int | None
    ) -> tuple[InboxCard, bool]:
        card_id = _inbox_card_id(self.allowlist, idempotency_key)
        client_msg_id = _client_msg_id(self.allowlist, f"inbox-card:{idempotency_key}", None)
        with self._lock:
            self._connection.execute("BEGIN IMMEDIATE")
            try:
                row = self._connection.execute(
                    "SELECT * FROM slack_inbox_cards WHERE idempotency_key = ?", (idempotency_key,)
                ).fetchone()
                if row is not None:
                    if responsibility_id is not None:
                        linked = self._connection.execute(
                            "SELECT attention_card_id FROM responsibilities WHERE id = ?", (responsibility_id,)
                        ).fetchone()
                        if linked is None or linked["attention_card_id"] != row["id"]:
                            raise SlackServiceError("idempotency key is bound to a different responsibility")
                    self._connection.execute("COMMIT")
                    return _inbox_card(row), False
                now = time.time()
                self._connection.execute(
                    """
                    INSERT INTO slack_inbox_cards (
                        id, idempotency_key, client_msg_id, channel_id, conversation_url,
                        title, body, state, revisit_at, version, delivery_state, slack_ts,
                        responsibility_id, render_state, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', NULL, 1, 'prepared', NULL, ?, 'rendered', ?, ?)
                    """,
                    (
                        card_id,
                        idempotency_key,
                        client_msg_id,
                        self.allowlist.channel_id,
                        conversation_url,
                        title,
                        body,
                        responsibility_id,
                        now,
                        now,
                    ),
                )
                if responsibility_id is not None:
                    link_attention_card(self._connection, responsibility_id, card_id, now, expected_version=responsibility_version)
                self._connection.execute("COMMIT")
                return self._get_inbox_card(card_id), True
            except Exception:
                if self._connection.in_transaction:
                    self._connection.execute("ROLLBACK")
                raise

    def _get_inbox_card(self, card_id: str) -> InboxCard:
        row = self._connection.execute("SELECT * FROM slack_inbox_cards WHERE id = ?", (card_id,)).fetchone()
        if row is None:
            raise SlackServiceError("unknown inbox card")
        return _inbox_card(row)

    def _get_inbox_card_by_key(self, idempotency_key: str) -> InboxCard:
        row = self._connection.execute(
            "SELECT * FROM slack_inbox_cards WHERE idempotency_key = ?", (idempotency_key,)
        ).fetchone()
        if row is None:
            raise SlackServiceError("unknown inbox card idempotency key")
        return _inbox_card(row)

    def _card_nudge_key(self, card_id: str) -> str | None:
        row = self._connection.execute(
            "SELECT resurface_nudge_key FROM slack_inbox_cards WHERE id = ?", (card_id,)
        ).fetchone()
        if row is None:
            raise SlackServiceError("unknown inbox card")
        value = row["resurface_nudge_key"]
        return str(value) if value is not None else None

    def _card_nudge_sent(self, card_id: str) -> bool:
        row = self._connection.execute(
            "SELECT resurface_nudge_sent_at FROM slack_inbox_cards WHERE id = ?", (card_id,)
        ).fetchone()
        if row is None:
            raise SlackServiceError("unknown inbox card")
        return row["resurface_nudge_sent_at"] is not None

    def _set_card_delivery(self, card_id: str, delivery_state: str, slack_ts: str | None) -> None:
        if delivery_state not in {"prepared", "pending_uncertain", "sent"}:
            raise ValueError("invalid inbox card delivery state")
        with self._lock:
            self._connection.execute(
                """
                UPDATE slack_inbox_cards
                SET delivery_state = ?, slack_ts = ?, updated_at = ?
                WHERE id = ?
                """,
                (delivery_state, slack_ts, time.time(), card_id),
            )

    def _update_inbox_card(self, card: InboxCard) -> None:
        if card.delivery_state != "sent" or card.slack_ts is None:
            raise SlackServiceError("inbox card has not been confirmed in Slack")
        try:
            response = _response_data(
                getattr(self._web_client, "chat_update")(
                    channel=self.allowlist.channel_id,
                    ts=card.slack_ts,
                    text=_card_fallback_text(card),
                    blocks=_card_blocks(card),
                )
            )
        except Exception as error:
            self._log_sdk_failure("Slack inbox card update failed", error)
            raise SlackServiceError("Slack inbox card update failed") from None
        if response is None or response.get("ok") is False:
            raise SlackServiceError("Slack rejected inbox card update")

    def _get_outbox(self, idempotency_key: str) -> OutboxEntry:
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM slack_outbox WHERE idempotency_key = ?", (idempotency_key,)
            ).fetchone()
        if row is None:
            raise SlackServiceError("unknown outgoing idempotency key")
        return _outbox_entry(row)

    def _set_outbox_state(self, idempotency_key: str, state: str, slack_ts: str | None) -> None:
        if state not in {"prepared", "pending_uncertain", "sent"}:
            raise ValueError("invalid outbox state")
        with self._lock:
            self._connection.execute(
                "UPDATE slack_outbox SET state = ?, slack_ts = ?, updated_at = ? WHERE idempotency_key = ?",
                (state, slack_ts, time.time(), idempotency_key),
            )

    def _find_sent_client_message(self, entry: OutboxEntry) -> str | None:
        if entry.thread_ts is None:
            return self._find_client_message("conversations_history", entry, channel=entry.channel_id, limit=200)
        return self._find_client_message(
            "conversations_replies", entry, channel=entry.channel_id, ts=entry.thread_ts, limit=200
        )

    def _outbox_thread_roots(self) -> set[str]:
        """Return roots created or used by confirmed Director outgoing messages."""

        with self._lock:
            rows = self._connection.execute(
                "SELECT thread_ts, slack_ts FROM slack_outbox WHERE state = 'sent'"
            ).fetchall()
        roots: set[str] = set()
        for row in rows:
            root = _slack_ts(row["thread_ts"]) or _slack_ts(row["slack_ts"])
            if root is not None:
                roots.add(root)
        return roots

    def _find_client_message(self, method_name: str, entry: OutboxEntry, **kwargs: Any) -> str | None:
        cursor: str | None = None
        seen_cursors: set[str] = set()
        while True:
            request = dict(kwargs)
            if cursor:
                request["cursor"] = cursor
            response = self._slack_call(method_name, **request)
            self._verify_response_channel(response)
            messages = response.get("messages")
            if not isinstance(messages, list):
                raise SlackServiceError("Slack reconciliation did not return messages")
            for raw in messages:
                message = _mapping(raw)
                if _is_confirmed_outgoing_message(message, entry.client_msg_id):
                    return _slack_ts(message.get("ts"))
            cursor = _next_cursor(response)
            if response.get("has_more") is True and not cursor:
                raise SlackServiceError("Slack reconciliation ended before all pages were returned")
            if not cursor:
                return None
            if cursor in seen_cursors:
                raise SlackServiceError("Slack reconciliation repeated a cursor")
            seen_cursors.add(cursor)

    def _initialize_outbox(self) -> None:
        with self._lock:
            initialize_schema(self._connection)
            self._connection.execute(
                """
                CREATE TABLE IF NOT EXISTS slack_outbox (
                    idempotency_key TEXT PRIMARY KEY,
                    client_msg_id TEXT NOT NULL UNIQUE,
                    channel_id TEXT NOT NULL,
                    thread_ts TEXT,
                    text TEXT NOT NULL,
                    state TEXT NOT NULL CHECK (state IN ('prepared', 'pending_uncertain', 'sent')),
                    slack_ts TEXT,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                )
                """
            )
            self._connection.execute(
                """
                CREATE TABLE IF NOT EXISTS slack_inbox_cards (
                    id TEXT PRIMARY KEY,
                    responsibility_id TEXT,
                    idempotency_key TEXT NOT NULL UNIQUE,
                    client_msg_id TEXT NOT NULL UNIQUE,
                    channel_id TEXT NOT NULL,
                    conversation_url TEXT NOT NULL,
                    title TEXT NOT NULL,
                    body TEXT NOT NULL,
                    state TEXT NOT NULL CHECK (state IN ('pending', 'deferred', 'dropped')),
                    revisit_at REAL,
                    version INTEGER NOT NULL CHECK (version > 0),
                    delivery_state TEXT NOT NULL CHECK (delivery_state IN ('prepared', 'pending_uncertain', 'sent')),
                    slack_ts TEXT,
                    render_state TEXT NOT NULL CHECK (render_state IN ('pending', 'rendered')),
                    resurface_cycle INTEGER NOT NULL DEFAULT 0,
                    resurface_nudge_key TEXT,
                    resurface_nudge_sent_at REAL,
                    resolution_state TEXT NOT NULL DEFAULT 'open',
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                )
                """
            )
            columns = {
                str(row["name"])
                for row in self._connection.execute("PRAGMA table_info(slack_inbox_cards)").fetchall()
            }
            for name, definition in (
                ("responsibility_id", "TEXT"),
                ("resurface_cycle", "INTEGER NOT NULL DEFAULT 0"),
                ("resurface_nudge_key", "TEXT"),
                ("resurface_nudge_sent_at", "REAL"),
                ("resolution_state", "TEXT NOT NULL DEFAULT 'open'"),
            ):
                if name not in columns:
                    self._connection.execute(f"ALTER TABLE slack_inbox_cards ADD COLUMN {name} {definition}")
            self._connection.execute(
                """
                CREATE TABLE IF NOT EXISTS slack_inbox_card_actions (
                    action_ts TEXT PRIMARY KEY,
                    card_id TEXT NOT NULL,
                    action_id TEXT NOT NULL,
                    changed INTEGER NOT NULL CHECK (changed IN (0, 1)),
                    created_at REAL NOT NULL
                )
                """
            )

    def _log_sdk_failure(self, message: str, error: Exception) -> None:
        # SDK exception messages/responses can hold private content or headers.
        self._log.error("%s (%s)", message, type(error).__name__)


def _response_data(value: object) -> Mapping[str, Any] | None:
    return _mapping(value) or _mapping(getattr(value, "data", None))


def _mapping(value: object) -> Mapping[str, Any] | None:
    return value if isinstance(value, Mapping) else None


def _slack_ts(value: object) -> str | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        float(value)
    except ValueError:
        return None
    return value


def _next_cursor(response: Mapping[str, Any]) -> str | None:
    metadata = _mapping(response.get("response_metadata"))
    cursor = metadata.get("next_cursor") if metadata is not None else None
    return cursor if isinstance(cursor, str) and cursor else None


def _successful_send_ts(response: Mapping[str, Any] | None) -> str | None:
    if response is None or response.get("ok") is False:
        return None
    message = _mapping(response.get("message"))
    return _slack_ts(message.get("ts")) if message is not None else _slack_ts(response.get("ts"))


def _canonical_source_revision(message: Mapping[str, Any]) -> str | None:
    edited = _mapping(message.get("edited"))
    return _slack_ts(edited.get("ts")) if edited is not None else _slack_ts(message.get("ts"))


def _is_confirmed_outgoing_message(message: Mapping[str, Any] | None, client_msg_id: str) -> bool:
    if message is None or message.get("client_msg_id") != client_msg_id:
        return False
    # `client_msg_id` is deterministic and scoped to this outbox.  Require a
    # Slack bot marker as well, so a user-authored lookalike cannot settle an
    # uncertain outbox row during a history scan.
    return bool(message.get("bot_id")) or message.get("subtype") == "bot_message"


def _already_reacted(error: Exception) -> bool:
    response = _response_data(getattr(error, "response", None))
    return response is not None and response.get("error") == "already_reacted"


def _client_msg_id(allowlist: SlackAllowlist, idempotency_key: str, thread_ts: str | None) -> str:
    value = f"director:slack:{allowlist.team_id}:{allowlist.channel_id}:{thread_ts or ''}:{idempotency_key}"
    return str(uuid.uuid5(uuid.NAMESPACE_URL, value))


def _recovery_event_id(
    allowlist: SlackAllowlist, source_ts: str, event_ts: str, event_type: str
) -> str:
    value = "\x1f".join((allowlist.team_id, allowlist.channel_id, source_ts, event_ts, event_type))
    return f"recovery:{hashlib.sha256(value.encode()).hexdigest()}"


def _validate_card_input(
    title: object, body: object, conversation_url: object, idempotency_key: object, allowlist: SlackAllowlist
) -> None:
    if not all(isinstance(value, str) and value.strip() for value in (title, body, conversation_url, idempotency_key)):
        raise ValueError("inbox card title, body, conversation URL, and idempotency key must be non-empty strings")
    if len(str(title)) > 100:
        raise ValueError("inbox card title must be at most 100 characters")
    if len(str(body)) > 3000:
        raise ValueError("inbox card body must be at most 3000 characters")
    if len(str(conversation_url)) > 3000:
        raise ValueError("inbox card conversation URL must be at most 3000 characters")
    parsed = urlparse(str(conversation_url))
    parts = parsed.path.split("/")
    query = parse_qs(parsed.query, strict_parsing=True)
    if (
        parsed.scheme != "https"
        or parsed.netloc != allowlist.workspace_domain
        or len(parts) < 4
        or parts[1] != "archives"
        or parts[2] != allowlist.channel_id
        or not (parts[3].startswith("p") and parts[3][1:].isdigit())
        or query.get("cid") != [allowlist.channel_id]
        or len(query.get("thread_ts", [])) != 1
        or _slack_ts(query["thread_ts"][0]) is None
    ):
        raise ValueError("inbox card conversation URL must be an allowlisted workspace thread permalink")


def _inbox_card_id(allowlist: SlackAllowlist, idempotency_key: str) -> str:
    value = f"director:inbox-card:{allowlist.team_id}:{allowlist.channel_id}:{idempotency_key}"
    return str(uuid.uuid5(uuid.NAMESPACE_URL, value))


def _resurface_nudge_key(card_id: str, cycle: int) -> str:
    return f"director-inbox-card-resurface:{card_id}:{cycle}"


def _inbox_card_permalink(card: InboxCard, allowlist: SlackAllowlist) -> str:
    if card.slack_ts is None or allowlist.workspace_domain is None:
        raise SlackServiceError("cannot create inbox card permalink without a verified Slack message and workspace")
    return f"https://{allowlist.workspace_domain}/archives/{card.channel_id}/p{card.slack_ts.replace('.', '')}"


def _resurface_nudge_text(card: InboxCard, allowlist: SlackAllowlist) -> str:
    link = _inbox_card_permalink(card, allowlist)
    return f"<@{allowlist.owner_user_id}> {card.title} is ready again. <{link}|Open the action card>."


def _card_fallback_text(card: InboxCard) -> str:
    if card.resolution_state == "completed":
        return f"{card.title}: completed."
    if card.resolution_state == "continued":
        return f"{card.title}: continuing in the conversation."
    if card.resolution_state == "cancelled":
        return f"{card.title}: no longer tracking this item."
    if card.state == "dropped":
        return f"{card.title}: no longer tracking this item."
    if card.state == "deferred" and card.revisit_at is not None:
        return f"{card.title}: set aside until {time.strftime('%b %-d', time.localtime(card.revisit_at))}."
    return f"{card.title}: {card.body}"


def _card_blocks(card: InboxCard) -> list[dict[str, object]]:
    blocks: list[dict[str, object]] = [
        {"type": "header", "text": {"type": "plain_text", "text": card.title, "emoji": True}},
        {"type": "section", "text": {"type": "mrkdwn", "text": card.body}},
    ]
    if card.resolution_state in {"completed", "continued", "cancelled"}:
        label = {"completed": "completed", "continued": "continuing in the conversation", "cancelled": "no longer tracking"}[card.resolution_state]
        blocks.extend(
            [
                {"type": "context", "elements": [{"type": "mrkdwn", "text": f"*Director inbox* · {label}"}]},
                {"type": "actions", "elements": [{"type": "button", "text": {"type": "plain_text", "text": "Open conversation", "emoji": True}, "url": card.conversation_url, "action_id": "director_open_conversation", "value": f"{card.id}:{card.version}"}]},
            ]
        )
    elif card.state == "pending":
        options = (
            ("In 1 day", 86400),
            ("In 3 days", 3 * 86400),
            ("In 1 week", 7 * 86400),
        )
        blocks.extend(
            [
                {
                    "type": "context",
                    "elements": [{"type": "mrkdwn", "text": "*Director inbox* · ready for your attention"}],
                },
                {
                    "type": "actions",
                    "elements": [
                        {
                            "type": "button",
                            "text": {"type": "plain_text", "text": "Open conversation", "emoji": True},
                            "url": card.conversation_url,
                            "action_id": "director_open_conversation",
                            "value": f"{card.id}:{card.version}",
                        },
                        {
                            "type": "static_select",
                            "placeholder": {"type": "plain_text", "text": "Later", "emoji": True},
                            "action_id": "director_later",
                            "options": [
                                {
                                    "text": {"type": "plain_text", "text": label, "emoji": True},
                                    "value": f"{card.id}:{card.version}:{int(delay_seconds)}",
                                }
                                for label, delay_seconds in options
                            ],
                        },
                        {
                            "type": "button",
                            "text": {"type": "plain_text", "text": "Drop", "emoji": True},
                            "style": "danger",
                            "action_id": "director_drop",
                            "value": f"{card.id}:{card.version}",
                            "confirm": {
                                "title": {"type": "plain_text", "text": f"Drop {card.title}?"},
                                "text": {"type": "mrkdwn", "text": "Director will stop tracking this item."},
                                "confirm": {"type": "plain_text", "text": "Drop"},
                                "deny": {"type": "plain_text", "text": "Keep it"},
                            },
                        },
                    ],
                },
            ]
        )
    elif card.state == "deferred" and card.revisit_at is not None:
        blocks.extend(
            [
                {
                    "type": "context",
                    "elements": [
                        {"type": "mrkdwn", "text": f"*Director inbox* · set aside until {time.strftime('%b %-d', time.localtime(card.revisit_at))}"}
                    ],
                },
                {
                    "type": "actions",
                    "elements": [
                        {
                            "type": "button",
                            "text": {"type": "plain_text", "text": "Open conversation", "emoji": True},
                            "url": card.conversation_url,
                            "action_id": "director_open_conversation",
                            "value": f"{card.id}:{card.version}",
                        }
                    ],
                },
            ]
        )
    else:
        blocks.extend(
            [
                {
                    "type": "context",
                    "elements": [{"type": "mrkdwn", "text": "*Director inbox* · no longer tracking"}],
                },
                {
                    "type": "actions",
                    "elements": [
                        {
                            "type": "button",
                            "text": {"type": "plain_text", "text": "Open conversation", "emoji": True},
                            "url": card.conversation_url,
                            "action_id": "director_open_conversation",
                            "value": f"{card.id}:{card.version}",
                        }
                    ],
                },
            ]
        )
    return blocks


def _parse_card_interaction(
    payload: Mapping[str, Any], allowlist: SlackAllowlist
) -> tuple[str, str, str, int, float | None, str] | None:
    if payload.get("type") != "block_actions":
        return None
    team = _mapping(payload.get("team"))
    channel = _mapping(payload.get("channel"))
    user = _mapping(payload.get("user"))
    if (
        team is None
        or channel is None
        or user is None
        or team.get("id") != allowlist.team_id
        or channel.get("id") != allowlist.channel_id
        or user.get("id") != allowlist.owner_user_id
    ):
        return None
    container = _mapping(payload.get("container"))
    message = _mapping(payload.get("message"))
    message_ts = _slack_ts(container.get("message_ts")) if container is not None else None
    if message_ts is None and message is not None:
        message_ts = _slack_ts(message.get("ts"))
    if message_ts is None:
        return None
    actions = payload.get("actions")
    if not isinstance(actions, list) or len(actions) != 1:
        return None
    action = _mapping(actions[0])
    if action is None:
        return None
    action_id = action.get("action_id")
    action_ts = _slack_ts(action.get("action_ts"))
    if action_id not in {"director_open_conversation", "director_later", "director_drop"} or action_ts is None:
        return None
    value = action.get("value")
    if action_id == "director_later":
        selected = _mapping(action.get("selected_option"))
        value = selected.get("value") if selected is not None else None
    if not isinstance(value, str):
        return None
    parts = value.split(":")
    expected_parts = 3 if action_id == "director_later" else 2
    if len(parts) != expected_parts or not parts[0] or not parts[1].isdigit():
        return None
    defer_seconds: float | None = None
    if action_id == "director_later":
        if not parts[2].isdigit():
            return None
        defer_seconds = float(parts[2])
        if defer_seconds not in {86400.0, 259200.0, 604800.0}:
            return None
    return action_id, action_ts, parts[0], int(parts[1]), defer_seconds, message_ts


def _outbox_entry(row: sqlite3.Row) -> OutboxEntry:
    return OutboxEntry(
        idempotency_key=str(row["idempotency_key"]),
        client_msg_id=str(row["client_msg_id"]),
        channel_id=str(row["channel_id"]),
        thread_ts=row["thread_ts"],
        text=str(row["text"]),
        state=str(row["state"]),
        slack_ts=row["slack_ts"],
    )


def _inbox_card(row: sqlite3.Row) -> InboxCard:
    revisit_at = row["revisit_at"]
    return InboxCard(
        id=str(row["id"]),
        idempotency_key=str(row["idempotency_key"]),
        client_msg_id=str(row["client_msg_id"]),
        channel_id=str(row["channel_id"]),
        conversation_url=str(row["conversation_url"]),
        title=str(row["title"]),
        body=str(row["body"]),
        state=str(row["state"]),
        revisit_at=float(revisit_at) if revisit_at is not None else None,
        version=int(row["version"]),
        delivery_state=str(row["delivery_state"]),
        slack_ts=row["slack_ts"],
        resolution_state=str(row["resolution_state"]),
    )


def _outgoing_result(entry: OutboxEntry) -> OutgoingResult:
    return OutgoingResult(entry.idempotency_key, entry.client_msg_id, entry.state, entry.slack_ts)


def _positive_int(value: object) -> int | None:
    return value if isinstance(value, int) and value > 0 else None


def _same_message_revision(left: object, right: object) -> bool:
    return (
        getattr(left, "id", None) == getattr(right, "id", None)
        and getattr(left, "revision", None) == getattr(right, "revision", None)
        and getattr(left, "source_ts", None) == getattr(right, "source_ts", None)
        and getattr(left, "event_ts", None) == getattr(right, "event_ts", None)
        and getattr(left, "thread_ts", None) == getattr(right, "thread_ts", None)
    )
