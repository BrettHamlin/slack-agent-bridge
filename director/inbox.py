"""A small SQLite-backed inbox for durable references to external messages.

The inbox deliberately stores identifiers and timestamps only.  Fetching or
rendering message contents belongs to the transport layer, outside this module.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
import math
from pathlib import Path
import secrets
import sqlite3
import threading
import time
from typing import Iterator


@dataclass(frozen=True)
class InboundPointer:
    """The stable, non-content reference for one source message event."""

    event_id: str
    source_team_id: str
    source_channel_id: str
    source_ts: str
    event_ts: str
    thread_ts: str | None = None
    event_type: str = "message"
    received_at: float | None = None


@dataclass(frozen=True)
class InboxMessage:
    id: int
    source_team_id: str
    source_channel_id: str
    source_ts: str
    event_ts: str
    thread_ts: str | None
    event_type: str
    revision: int
    ingested_at: float
    updated_at: float


@dataclass(frozen=True)
class IngestResult:
    message: InboxMessage
    created: bool
    duplicate_event: bool
    source_updated: bool


@dataclass(frozen=True)
class RelayLease:
    """A fenced, time-limited right to relay one specific message revision."""

    message_id: int
    source_team_id: str
    source_channel_id: str
    source_ts: str
    event_ts: str
    thread_ts: str | None
    event_type: str
    revision: int
    holder: str
    token: str
    expires_at: float


@dataclass(frozen=True)
class IntakeAckLease:
    """A fenced right to add the persisted-intake reaction for one revision."""

    message_id: int
    source_team_id: str
    source_channel_id: str
    source_ts: str
    revision: int
    holder: str
    token: str
    expires_at: float


@dataclass(frozen=True)
class ReceiptState:
    ingested_at: float
    relayed_revisions: tuple[int, ...]
    read_at: float | None
    completed_at: float | None
    read_revisions: tuple[int, ...]
    completed_revisions: tuple[int, ...]

    def is_relayed(self, revision: int) -> bool:
        return revision in self.relayed_revisions


@dataclass(frozen=True)
class PendingSource:
    """A current source pointer awaiting relay, without taking its lease."""

    message: InboxMessage
    receipts: ReceiptState
    lease_holder: str | None
    lease_expires_at: float | None


@dataclass(frozen=True)
class SourceThread:
    """A source conversation root that can be revisited during recovery."""

    source_team_id: str
    source_channel_id: str
    thread_ts: str
    first_seen_at: float
    last_seen_at: float


@dataclass(frozen=True)
class Checkpoint:
    name: str
    value: str
    created_at: float
    updated_at: float


class InboxError(RuntimeError):
    """Base error for durable inbox operations."""


class UnknownMessage(InboxError):
    pass


class LeaseLost(InboxError):
    """The lease expired, was superseded, or belongs to another worker."""


class InboxStore:
    """Durable source pointers and independent processing receipts.

    SQLite's ``BEGIN IMMEDIATE`` is used when claiming or acknowledging a lease.
    This makes a relay handoff atomic across processes sharing the database.
    """

    def __init__(self, path: str | Path) -> None:
        self.path = str(path)
        self._lock = threading.RLock()
        self._connection = sqlite3.connect(
            self.path,
            isolation_level=None,
            check_same_thread=False,
        )
        self._connection.row_factory = sqlite3.Row
        self._connection.execute("PRAGMA foreign_keys = ON")
        self._connection.execute("PRAGMA busy_timeout = 5000")
        self._initialize()

    def close(self) -> None:
        with self._lock:
            self._connection.close()

    def __enter__(self) -> "InboxStore":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def ingest(self, pointer: InboundPointer) -> IngestResult:
        """Record an event, preserving a stable source message id across edits.

        A repeated event id is a no-op.  A new event for an existing source
        pointer advances its revision only when it is at least as new as the
        stored Slack event timestamp and its pointer metadata changed.
        A revision clears any outstanding relay lease, so an old worker cannot
        acknowledge the newer source state.
        """
        self._validate_pointer(pointer)
        now = pointer.received_at if pointer.received_at is not None else time.time()
        with self._transaction():
            duplicate = self._connection.execute(
                "SELECT message_id FROM source_events WHERE event_id = ?",
                (pointer.event_id,),
            ).fetchone()
            self._track_thread(pointer, now)
            if duplicate is not None:
                row = self._message_row(int(duplicate["message_id"]))
                return IngestResult(self._to_message(row), False, True, False)

            row = self._connection.execute(
                """
                SELECT * FROM messages
                WHERE source_team_id = ? AND source_channel_id = ? AND source_ts = ?
                """,
                (pointer.source_team_id, pointer.source_channel_id, pointer.source_ts),
            ).fetchone()
            if row is None:
                cursor = self._connection.execute(
                    """
                    INSERT INTO messages (
                        source_team_id, source_channel_id, source_ts, event_ts,
                        thread_ts, event_type, revision, ingested_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?)
                    """,
                    (
                        pointer.source_team_id,
                        pointer.source_channel_id,
                        pointer.source_ts,
                        pointer.event_ts,
                        pointer.thread_ts,
                        pointer.event_type,
                        now,
                        now,
                    ),
                )
                message_id = int(cursor.lastrowid)
                self._connection.execute(
                    "INSERT INTO ingested_receipts (message_id, ingested_at) VALUES (?, ?)",
                    (message_id, now),
                )
                self._connection.execute(
                    "INSERT INTO source_events (event_id, message_id, event_ts, received_at) VALUES (?, ?, ?, ?)",
                    (pointer.event_id, message_id, pointer.event_ts, now),
                )
                if pointer.event_type != "message_deleted":
                    self._connection.execute(
                        """
                        INSERT INTO intake_acknowledgements (
                            message_id, revision, state, attempts, next_attempt_at, created_at, updated_at
                        ) VALUES (?, 1, 'pending', 0, ?, ?, ?)
                        """,
                        (message_id, now, now, now),
                    )
                return IngestResult(self._to_message(self._message_row(message_id)), True, False, False)

            message_id = int(row["id"])
            self._connection.execute(
                "INSERT INTO source_events (event_id, message_id, event_ts, received_at) VALUES (?, ?, ?, ?)",
                (pointer.event_id, message_id, pointer.event_ts, now),
            )
            current_event_ts = self._slack_timestamp(str(row["event_ts"]), "stored event_ts")
            incoming_event_ts = self._slack_timestamp(pointer.event_ts, "event_ts")
            stale = incoming_event_ts < current_event_ts
            changed = not stale and (
                incoming_event_ts > current_event_ts
                or row["thread_ts"] != pointer.thread_ts
                or row["event_type"] != pointer.event_type
            )
            if changed:
                self._connection.execute(
                    """
                    UPDATE messages
                    SET event_ts = ?, thread_ts = ?, event_type = ?,
                        revision = revision + 1, updated_at = ?,
                        lease_holder = NULL, lease_token = NULL,
                        lease_expires_at = NULL, lease_revision = NULL
                    WHERE id = ?
                    """,
                    (pointer.event_ts, pointer.thread_ts, pointer.event_type, now, message_id),
                )
                revised = self._message_row(message_id)
                if pointer.event_type == "message_deleted":
                    self._connection.execute(
                        """
                        UPDATE intake_acknowledgements
                        SET state = 'skipped', lease_holder = NULL, lease_token = NULL,
                            lease_expires_at = NULL, updated_at = ?
                        WHERE message_id = ? AND state != 'sent'
                        """,
                        (now, message_id),
                    )
                else:
                    # A message edit can arrive while the initial reaction is
                    # pending.  Fence that old lease and queue the current
                    # saved revision; an already-sent source reaction remains
                    # valid for the same Slack message timestamp.
                    self._connection.execute(
                        """
                        INSERT INTO intake_acknowledgements (
                            message_id, revision, state, attempts, next_attempt_at, created_at, updated_at
                        ) VALUES (?, ?, 'pending', 0, ?, ?, ?)
                        ON CONFLICT(message_id) DO UPDATE SET
                            revision = CASE
                                WHEN intake_acknowledgements.state = 'sent' THEN intake_acknowledgements.revision
                                ELSE excluded.revision
                            END,
                            state = CASE
                                WHEN intake_acknowledgements.state = 'sent' THEN 'sent'
                                ELSE 'pending'
                            END,
                            attempts = CASE
                                WHEN intake_acknowledgements.state = 'sent' THEN intake_acknowledgements.attempts
                                ELSE 0
                            END,
                            next_attempt_at = CASE
                                WHEN intake_acknowledgements.state = 'sent' THEN intake_acknowledgements.next_attempt_at
                                ELSE excluded.next_attempt_at
                            END,
                            lease_holder = NULL,
                            lease_token = NULL,
                            lease_expires_at = NULL,
                            updated_at = excluded.updated_at
                        """,
                        (message_id, int(revised["revision"]), now, now, now),
                    )
            return IngestResult(self._to_message(self._message_row(message_id)), False, False, changed)

    def claim_pending_intake_acks(
        self,
        holder: str,
        lease_seconds: float,
        source_team_id: str,
        source_channel_id: str,
        limit: int = 1,
        *,
        now: float | None = None,
    ) -> tuple[IntakeAckLease, ...]:
        """Claim queued intake reactions that are still current and trusted.

        The queue is deliberately separate from ``read_receipts``: these
        reactions confirm durable intake only, and do not imply a manager read
        or completed work.
        """
        if not holder:
            raise ValueError("holder must not be empty")
        if not isinstance(lease_seconds, (int, float)) or not math.isfinite(lease_seconds) or lease_seconds <= 0:
            raise ValueError("lease_seconds must be a finite positive number")
        if not source_team_id or not source_channel_id:
            raise ValueError("source identity must not be empty")
        if limit <= 0:
            raise ValueError("limit must be positive")
        claimed_at = time.time() if now is None else now
        expires_at = claimed_at + lease_seconds
        # The worker polls frequently for low reaction latency.  Avoid taking
        # an IMMEDIATE write lock when there is no due, expired, or invalid row
        # to recover; ordinary intake remains the only writer while idle.
        with self._lock:
            candidate = self._connection.execute(
                """
                SELECT 1
                FROM intake_acknowledgements AS a
                JOIN messages AS m ON m.id = a.message_id
                WHERE (a.state = 'pending' AND a.next_attempt_at <= ?)
                   OR (a.state = 'leased' AND a.lease_expires_at <= ?)
                   OR (
                        a.state IN ('pending', 'leased')
                        AND (
                            m.revision != a.revision
                            OR m.event_type = 'message_deleted'
                            OR m.source_team_id != ?
                            OR m.source_channel_id != ?
                        )
                   )
                LIMIT 1
                """,
                (claimed_at, claimed_at, source_team_id, source_channel_id),
            ).fetchone()
        if candidate is None:
            return ()
        with self._transaction():
            self._connection.execute(
                """
                UPDATE intake_acknowledgements
                SET state = 'pending', lease_holder = NULL, lease_token = NULL,
                    lease_expires_at = NULL, next_attempt_at = ?, updated_at = ?
                WHERE state = 'leased' AND lease_expires_at <= ?
                """,
                (claimed_at, claimed_at, claimed_at),
            )
            # Never react to a source that was deleted, superseded, or belongs
            # to another channel.  This also clears interrupted stale leases.
            self._connection.execute(
                """
                UPDATE intake_acknowledgements AS a
                SET state = 'skipped', lease_holder = NULL, lease_token = NULL,
                    lease_expires_at = NULL, updated_at = ?
                WHERE a.state IN ('pending', 'leased')
                  AND EXISTS (
                    SELECT 1 FROM messages AS m
                    WHERE m.id = a.message_id AND (
                        m.revision != a.revision
                        OR m.event_type = 'message_deleted'
                        OR m.source_team_id != ?
                        OR m.source_channel_id != ?
                    )
                  )
                """,
                (claimed_at, source_team_id, source_channel_id),
            )
            candidates = self._connection.execute(
                """
                SELECT a.message_id, a.revision, m.source_team_id, m.source_channel_id, m.source_ts
                FROM intake_acknowledgements AS a
                JOIN messages AS m ON m.id = a.message_id
                WHERE a.state = 'pending'
                  AND a.next_attempt_at <= ?
                  AND m.revision = a.revision
                  AND m.event_type != 'message_deleted'
                  AND m.source_team_id = ?
                  AND m.source_channel_id = ?
                ORDER BY a.message_id
                LIMIT ?
                """,
                (claimed_at, source_team_id, source_channel_id, limit),
            ).fetchall()
            leases: list[IntakeAckLease] = []
            for row in candidates:
                token = secrets.token_urlsafe(24)
                self._connection.execute(
                    """
                    UPDATE intake_acknowledgements
                    SET state = 'leased', lease_holder = ?, lease_token = ?,
                        lease_expires_at = ?, updated_at = ?
                    WHERE message_id = ? AND state = 'pending'
                    """,
                    (holder, token, expires_at, claimed_at, row["message_id"]),
                )
                leases.append(
                    IntakeAckLease(
                        message_id=int(row["message_id"]),
                        source_team_id=str(row["source_team_id"]),
                        source_channel_id=str(row["source_channel_id"]),
                        source_ts=str(row["source_ts"]),
                        revision=int(row["revision"]),
                        holder=holder,
                        token=token,
                        expires_at=expires_at,
                    )
                )
            return tuple(leases)

    def acknowledge_intake_ack(self, lease: IntakeAckLease, *, now: float | None = None) -> bool:
        """Settle a reaction only when its exact source revision remains valid."""
        acknowledged_at = time.time() if now is None else now
        with self._transaction():
            ack = self._connection.execute(
                "SELECT * FROM intake_acknowledgements WHERE message_id = ?", (lease.message_id,)
            ).fetchone()
            if (
                ack is None
                or ack["state"] != "leased"
                or int(ack["revision"]) != lease.revision
                or ack["lease_holder"] != lease.holder
                or ack["lease_token"] != lease.token
                or ack["lease_expires_at"] is None
                or float(ack["lease_expires_at"]) <= acknowledged_at
            ):
                return False
            message = self._message_row(lease.message_id)
            current = (
                int(message["revision"]) == lease.revision
                and message["event_type"] != "message_deleted"
                and message["source_team_id"] == lease.source_team_id
                and message["source_channel_id"] == lease.source_channel_id
            )
            state = "sent" if current else "skipped"
            self._connection.execute(
                """
                UPDATE intake_acknowledgements
                SET state = ?, lease_holder = NULL, lease_token = NULL,
                    lease_expires_at = NULL, updated_at = ?
                WHERE message_id = ?
                """,
                (state, acknowledged_at, lease.message_id),
            )
            return current

    def validate_intake_ack(self, lease: IntakeAckLease, *, now: float | None = None) -> bool:
        """Fence a claimed reaction immediately before its external Slack call."""
        checked_at = time.time() if now is None else now
        with self._transaction():
            ack = self._connection.execute(
                "SELECT * FROM intake_acknowledgements WHERE message_id = ?", (lease.message_id,)
            ).fetchone()
            if (
                ack is None
                or ack["state"] != "leased"
                or int(ack["revision"]) != lease.revision
                or ack["lease_holder"] != lease.holder
                or ack["lease_token"] != lease.token
            ):
                return False
            if ack["lease_expires_at"] is None or float(ack["lease_expires_at"]) <= checked_at:
                self._connection.execute(
                    """
                    UPDATE intake_acknowledgements
                    SET state = 'pending', lease_holder = NULL, lease_token = NULL,
                        lease_expires_at = NULL, next_attempt_at = ?, updated_at = ?
                    WHERE message_id = ?
                    """,
                    (checked_at, checked_at, lease.message_id),
                )
                return False
            message = self._message_row(lease.message_id)
            current = (
                int(message["revision"]) == lease.revision
                and message["event_type"] != "message_deleted"
                and message["source_team_id"] == lease.source_team_id
                and message["source_channel_id"] == lease.source_channel_id
            )
            if not current:
                self._connection.execute(
                    """
                    UPDATE intake_acknowledgements
                    SET state = 'skipped', lease_holder = NULL, lease_token = NULL,
                        lease_expires_at = NULL, updated_at = ?
                    WHERE message_id = ?
                    """,
                    (checked_at, lease.message_id),
                )
            return current

    def retry_intake_ack(self, lease: IntakeAckLease, *, now: float | None = None) -> bool:
        """Release a failed reaction for bounded backoff retry, if still current."""
        failed_at = time.time() if now is None else now
        with self._transaction():
            ack = self._connection.execute(
                "SELECT * FROM intake_acknowledgements WHERE message_id = ?", (lease.message_id,)
            ).fetchone()
            if (
                ack is None
                or ack["state"] != "leased"
                or int(ack["revision"]) != lease.revision
                or ack["lease_holder"] != lease.holder
                or ack["lease_token"] != lease.token
            ):
                return False
            message = self._message_row(lease.message_id)
            current = (
                int(message["revision"]) == lease.revision
                and message["event_type"] != "message_deleted"
                and message["source_team_id"] == lease.source_team_id
                and message["source_channel_id"] == lease.source_channel_id
            )
            if not current:
                self._connection.execute(
                    """
                    UPDATE intake_acknowledgements
                    SET state = 'skipped', lease_holder = NULL, lease_token = NULL,
                        lease_expires_at = NULL, updated_at = ?
                    WHERE message_id = ?
                    """,
                    (failed_at, lease.message_id),
                )
                return False
            attempts = int(ack["attempts"]) + 1
            delay = min(60.0, float(2 ** min(attempts, 6)))
            self._connection.execute(
                """
                UPDATE intake_acknowledgements
                SET state = 'pending', attempts = ?, next_attempt_at = ?,
                    lease_holder = NULL, lease_token = NULL, lease_expires_at = NULL,
                    updated_at = ?
                WHERE message_id = ?
                """,
                (attempts, failed_at + delay, failed_at, lease.message_id),
            )
            return True

    def release_intake_acks(self, holder: str, *, now: float | None = None) -> None:
        """Return this worker's in-flight reactions to the durable queue."""
        released_at = time.time() if now is None else now
        with self._transaction():
            self._connection.execute(
                """
                UPDATE intake_acknowledgements
                SET state = 'pending', lease_holder = NULL, lease_token = NULL,
                    lease_expires_at = NULL, next_attempt_at = ?, updated_at = ?
                WHERE state = 'leased' AND lease_holder = ?
                """,
                (released_at, released_at, holder),
            )

    def intake_ack_state(self, message_id: int) -> str | None:
        """Return the durable intake-ack state for focused diagnostics/tests."""
        with self._lock:
            row = self._connection.execute(
                "SELECT state FROM intake_acknowledgements WHERE message_id = ?", (message_id,)
            ).fetchone()
            return None if row is None else str(row["state"])

    def claim_pending_relay(
        self,
        holder: str,
        lease_seconds: float,
        limit: int = 1,
        *,
        now: float | None = None,
    ) -> tuple[RelayLease, ...]:
        """Atomically claim up to ``limit`` actionable source revisions.

        A completion receipt suppresses relay only for the current revision;
        it does not manufacture a relay receipt or revoke an already claimed
        lease.  That lets an in-flight, valid delivery acknowledgement finish
        while preventing later recovery runs from re-delivering completed work.
        """
        if not holder:
            raise ValueError("holder must not be empty")
        if not isinstance(lease_seconds, (int, float)) or not math.isfinite(lease_seconds) or lease_seconds <= 0:
            raise ValueError("lease_seconds must be a finite positive number")
        if limit <= 0:
            raise ValueError("limit must be positive")
        claimed_at = time.time() if now is None else now
        expires_at = claimed_at + lease_seconds
        with self._transaction():
            candidates = self._connection.execute(
                """
                SELECT m.* FROM messages AS m
                WHERE NOT EXISTS (
                    SELECT 1 FROM relayed_receipts AS r
                    WHERE r.message_id = m.id AND r.revision = m.revision
                )
                AND NOT EXISTS (
                    SELECT 1 FROM completed_receipts AS c
                    WHERE c.message_id = m.id AND c.revision = m.revision
                )
                AND (m.lease_expires_at IS NULL OR m.lease_expires_at <= ?)
                ORDER BY m.id
                LIMIT ?
                """,
                (claimed_at, limit),
            ).fetchall()
            leases: list[RelayLease] = []
            for row in candidates:
                token = secrets.token_urlsafe(24)
                revision = int(row["revision"])
                self._connection.execute(
                    """
                    UPDATE messages
                    SET lease_holder = ?, lease_token = ?, lease_expires_at = ?, lease_revision = ?
                    WHERE id = ?
                    """,
                    (holder, token, expires_at, revision, row["id"]),
                )
                leases.append(
                    RelayLease(
                        message_id=int(row["id"]),
                        source_team_id=str(row["source_team_id"]),
                        source_channel_id=str(row["source_channel_id"]),
                        source_ts=str(row["source_ts"]),
                        event_ts=str(row["event_ts"]),
                        thread_ts=row["thread_ts"],
                        event_type=str(row["event_type"]),
                        revision=revision,
                        holder=holder,
                        token=token,
                        expires_at=expires_at,
                    )
                )
            return tuple(leases)

    def acknowledge_relay(self, lease: RelayLease, *, now: float | None = None) -> None:
        """Persist a relay receipt only if this exact lease is still valid."""
        acknowledged_at = time.time() if now is None else now
        with self._transaction():
            row = self._connection.execute(
                "SELECT * FROM messages WHERE id = ?",
                (lease.message_id,),
            ).fetchone()
            if (
                row is None
                or row["lease_holder"] != lease.holder
                or row["lease_token"] != lease.token
                or row["lease_expires_at"] is None
                or float(row["lease_expires_at"]) <= acknowledged_at
                or int(row["lease_revision"] or -1) != lease.revision
                or int(row["revision"]) != lease.revision
            ):
                raise LeaseLost(f"relay lease is no longer valid for message {lease.message_id}")
            self._connection.execute(
                """
                INSERT INTO relayed_receipts (message_id, revision, relayed_at)
                VALUES (?, ?, ?)
                ON CONFLICT(message_id, revision) DO NOTHING
                """,
                (lease.message_id, lease.revision, acknowledged_at),
            )
            self._connection.execute(
                """
                UPDATE messages
                SET lease_holder = NULL, lease_token = NULL,
                    lease_expires_at = NULL, lease_revision = NULL
                WHERE id = ?
                """,
                (lease.message_id,),
            )

    def mark_read(self, message_id: int, *, now: float | None = None) -> bool:
        """Record a read receipt. Returns whether it was newly recorded."""
        return self._record_terminal_receipt("read_receipts", "read_at", message_id, None, now)

    def mark_read_if_revision(
        self, message_id: int, revision: int, *, now: float | None = None
    ) -> bool:
        """Record a read only when ``revision`` is still the current revision."""
        return self._record_terminal_receipt("read_receipts", "read_at", message_id, revision, now)

    def mark_completed(self, message_id: int, *, now: float | None = None) -> bool:
        """Record a completion receipt. It is independent of the read receipt."""
        return self._record_terminal_receipt("completed_receipts", "completed_at", message_id, None, now)

    def mark_completed_if_revision(
        self, message_id: int, revision: int, *, now: float | None = None
    ) -> bool:
        """Record completion only when ``revision`` is still current."""
        return self._record_terminal_receipt("completed_receipts", "completed_at", message_id, revision, now)

    def get_message(self, message_id: int) -> InboxMessage:
        with self._lock:
            return self._to_message(self._message_row(message_id))

    def get_receipts(self, message_id: int) -> ReceiptState:
        with self._lock:
            message = self._message_row(message_id)
            ingested = self._connection.execute(
                "SELECT ingested_at FROM ingested_receipts WHERE message_id = ?", (message_id,)
            ).fetchone()
            relayed = self._connection.execute(
                "SELECT revision FROM relayed_receipts WHERE message_id = ? ORDER BY revision", (message_id,)
            ).fetchall()
            revision = int(message["revision"])
            read = self._connection.execute(
                "SELECT read_at FROM read_receipts WHERE message_id = ? AND revision = ?",
                (message_id, revision),
            ).fetchone()
            completed = self._connection.execute(
                "SELECT completed_at FROM completed_receipts WHERE message_id = ? AND revision = ?",
                (message_id, revision),
            ).fetchone()
            read_revisions = self._connection.execute(
                "SELECT revision FROM read_receipts WHERE message_id = ? ORDER BY revision", (message_id,)
            ).fetchall()
            completed_revisions = self._connection.execute(
                "SELECT revision FROM completed_receipts WHERE message_id = ? ORDER BY revision", (message_id,)
            ).fetchall()
            assert ingested is not None  # Schema invariant for every messages row.
            return ReceiptState(
                ingested_at=float(ingested["ingested_at"]),
                relayed_revisions=tuple(int(item["revision"]) for item in relayed),
                read_at=None if read is None else float(read["read_at"]),
                completed_at=None if completed is None else float(completed["completed_at"]),
                read_revisions=tuple(int(item["revision"]) for item in read_revisions),
                completed_revisions=tuple(int(item["revision"]) for item in completed_revisions),
            )

    def pending_relay_count(self, *, now: float | None = None) -> int:
        current_time = time.time() if now is None else now
        with self._lock:
            row = self._connection.execute(
                """
                SELECT COUNT(*) AS count FROM messages AS m
                WHERE NOT EXISTS (
                    SELECT 1 FROM relayed_receipts AS r
                    WHERE r.message_id = m.id AND r.revision = m.revision
                )
                AND NOT EXISTS (
                    SELECT 1 FROM completed_receipts AS c
                    WHERE c.message_id = m.id AND c.revision = m.revision
                )
                AND (m.lease_expires_at IS NULL OR m.lease_expires_at <= ?)
                """,
                (current_time,),
            ).fetchone()
            return int(row["count"])

    def list_pending_relays(
        self, *, include_leased: bool = True, now: float | None = None
    ) -> tuple[PendingSource, ...]:
        """List actionable current source revisions without acquiring leases.

        With the default, a coordinator can see work another worker currently
        owns. Completed current revisions are excluded even if they lack a
        relay receipt. ``include_leased=False`` limits the result to work
        claimable at ``now``.
        """
        current_time = time.time() if now is None else now
        lease_clause = "" if include_leased else "AND (m.lease_expires_at IS NULL OR m.lease_expires_at <= ?)"
        parameters: tuple[object, ...] = () if include_leased else (current_time,)
        with self._lock:
            rows = self._connection.execute(
                f"""
                SELECT m.* FROM messages AS m
                WHERE NOT EXISTS (
                    SELECT 1 FROM relayed_receipts AS r
                    WHERE r.message_id = m.id AND r.revision = m.revision
                )
                AND NOT EXISTS (
                    SELECT 1 FROM completed_receipts AS c
                    WHERE c.message_id = m.id AND c.revision = m.revision
                )
                {lease_clause}
                ORDER BY m.id
                """,
                parameters,
            ).fetchall()
            return tuple(
                PendingSource(
                    message=self._to_message(row),
                    receipts=self.get_receipts(int(row["id"])),
                    lease_holder=row["lease_holder"],
                    lease_expires_at=(
                        None if row["lease_expires_at"] is None else float(row["lease_expires_at"])
                    ),
                )
                for row in rows
            )

    def list_open_messages(self) -> tuple[InboxMessage, ...]:
        """List current revisions that have not received a completion receipt.

        Relay and read receipts intentionally do not remove an item from this
        manager-facing sweep; only completion for the current revision does.
        """
        with self._lock:
            rows = self._connection.execute(
                """
                SELECT m.* FROM messages AS m
                WHERE NOT EXISTS (
                    SELECT 1 FROM completed_receipts AS c
                    WHERE c.message_id = m.id AND c.revision = m.revision
                )
                ORDER BY m.id
                """
            ).fetchall()
            return tuple(self._to_message(row) for row in rows)

    def list_source_threads(self) -> tuple[SourceThread, ...]:
        """Return every observed source thread root, including top-level messages."""
        with self._lock:
            rows = self._connection.execute(
                """
                SELECT source_team_id, source_channel_id, thread_ts, first_seen_at, last_seen_at
                FROM source_threads
                ORDER BY source_team_id, source_channel_id, thread_ts
                """
            ).fetchall()
            return tuple(
                SourceThread(
                    source_team_id=str(row["source_team_id"]),
                    source_channel_id=str(row["source_channel_id"]),
                    thread_ts=str(row["thread_ts"]),
                    first_seen_at=float(row["first_seen_at"]),
                    last_seen_at=float(row["last_seen_at"]),
                )
                for row in rows
            )

    def get_checkpoint(self, name: str) -> Checkpoint | None:
        self._validate_checkpoint_name(name)
        with self._lock:
            row = self._connection.execute(
                "SELECT name, value, created_at, updated_at FROM checkpoints WHERE name = ?", (name,)
            ).fetchone()
            return None if row is None else self._to_checkpoint(row)

    def set_checkpoint(self, name: str, value: str, *, now: float | None = None) -> Checkpoint:
        """Atomically create or advance an opaque recovery checkpoint."""
        self._validate_checkpoint_name(name)
        if not isinstance(value, str):
            raise ValueError("checkpoint value must be a string")
        recorded_at = time.time() if now is None else now
        with self._transaction():
            self._connection.execute(
                """
                INSERT INTO checkpoints (name, value, created_at, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(name) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at
                """,
                (name, value, recorded_at, recorded_at),
            )
            row = self._connection.execute(
                "SELECT name, value, created_at, updated_at FROM checkpoints WHERE name = ?", (name,)
            ).fetchone()
            assert row is not None
            return self._to_checkpoint(row)

    def _record_terminal_receipt(
        self,
        table: str,
        column: str,
        message_id: int,
        expected_revision: int | None,
        now: float | None,
    ) -> bool:
        recorded_at = time.time() if now is None else now
        with self._transaction():
            message = self._message_row(message_id)
            revision = int(message["revision"])
            if expected_revision is not None and revision != expected_revision:
                return False
            cursor = self._connection.execute(
                f"INSERT INTO {table} (message_id, revision, {column}) VALUES (?, ?, ?) "
                "ON CONFLICT(message_id, revision) DO NOTHING",
                (message_id, revision, recorded_at),
            )
            return cursor.rowcount == 1

    def _message_row(self, message_id: int) -> sqlite3.Row:
        row = self._connection.execute("SELECT * FROM messages WHERE id = ?", (message_id,)).fetchone()
        if row is None:
            raise UnknownMessage(f"unknown message {message_id}")
        return row

    @staticmethod
    def _to_message(row: sqlite3.Row) -> InboxMessage:
        return InboxMessage(
            id=int(row["id"]),
            source_team_id=str(row["source_team_id"]),
            source_channel_id=str(row["source_channel_id"]),
            source_ts=str(row["source_ts"]),
            event_ts=str(row["event_ts"]),
            thread_ts=row["thread_ts"],
            event_type=str(row["event_type"]),
            revision=int(row["revision"]),
            ingested_at=float(row["ingested_at"]),
            updated_at=float(row["updated_at"]),
        )

    @staticmethod
    def _to_checkpoint(row: sqlite3.Row) -> Checkpoint:
        return Checkpoint(
            name=str(row["name"]),
            value=str(row["value"]),
            created_at=float(row["created_at"]),
            updated_at=float(row["updated_at"]),
        )

    def _track_thread(self, pointer: InboundPointer, observed_at: float) -> None:
        thread_ts = pointer.thread_ts or pointer.source_ts
        self._connection.execute(
            """
            INSERT INTO source_threads (
                source_team_id, source_channel_id, thread_ts, first_seen_at, last_seen_at
            ) VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(source_team_id, source_channel_id, thread_ts)
            DO UPDATE SET last_seen_at = excluded.last_seen_at
            """,
            (pointer.source_team_id, pointer.source_channel_id, thread_ts, observed_at, observed_at),
        )

    @contextmanager
    def _transaction(self) -> Iterator[None]:
        with self._lock:
            self._connection.execute("BEGIN IMMEDIATE")
            try:
                yield
            except BaseException:
                self._connection.execute("ROLLBACK")
                raise
            else:
                self._connection.execute("COMMIT")

    def _initialize(self) -> None:
        with self._lock:
            self._connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS messages (
                    id INTEGER PRIMARY KEY,
                    source_team_id TEXT NOT NULL,
                    source_channel_id TEXT NOT NULL,
                    source_ts TEXT NOT NULL,
                    event_ts TEXT NOT NULL,
                    thread_ts TEXT,
                    event_type TEXT NOT NULL,
                    revision INTEGER NOT NULL,
                    ingested_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    lease_holder TEXT,
                    lease_token TEXT,
                    lease_expires_at REAL,
                    lease_revision INTEGER,
                    UNIQUE(source_team_id, source_channel_id, source_ts)
                );

                CREATE TABLE IF NOT EXISTS source_events (
                    event_id TEXT PRIMARY KEY,
                    message_id INTEGER NOT NULL REFERENCES messages(id),
                    event_ts TEXT NOT NULL,
                    received_at REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS ingested_receipts (
                    message_id INTEGER PRIMARY KEY REFERENCES messages(id),
                    ingested_at REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS relayed_receipts (
                    message_id INTEGER NOT NULL REFERENCES messages(id),
                    revision INTEGER NOT NULL,
                    relayed_at REAL NOT NULL,
                    PRIMARY KEY(message_id, revision)
                );

                CREATE TABLE IF NOT EXISTS read_receipts (
                    message_id INTEGER NOT NULL REFERENCES messages(id),
                    revision INTEGER NOT NULL,
                    read_at REAL NOT NULL,
                    PRIMARY KEY(message_id, revision)
                );

                CREATE TABLE IF NOT EXISTS completed_receipts (
                    message_id INTEGER NOT NULL REFERENCES messages(id),
                    revision INTEGER NOT NULL,
                    completed_at REAL NOT NULL,
                    PRIMARY KEY(message_id, revision)
                );

                CREATE TABLE IF NOT EXISTS intake_acknowledgements (
                    message_id INTEGER PRIMARY KEY REFERENCES messages(id),
                    revision INTEGER NOT NULL,
                    state TEXT NOT NULL CHECK(state IN ('pending', 'leased', 'sent', 'skipped')),
                    attempts INTEGER NOT NULL,
                    next_attempt_at REAL NOT NULL,
                    lease_holder TEXT,
                    lease_token TEXT,
                    lease_expires_at REAL,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS source_threads (
                    source_team_id TEXT NOT NULL,
                    source_channel_id TEXT NOT NULL,
                    thread_ts TEXT NOT NULL,
                    first_seen_at REAL NOT NULL,
                    last_seen_at REAL NOT NULL,
                    PRIMARY KEY(source_team_id, source_channel_id, thread_ts)
                );

                CREATE TABLE IF NOT EXISTS checkpoints (
                    name TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                );
                """
            )
            self._migrate_terminal_receipts("read_receipts", "read_at")
            self._migrate_terminal_receipts("completed_receipts", "completed_at")
            self._connection.execute(
                """
                INSERT OR IGNORE INTO source_threads (
                    source_team_id, source_channel_id, thread_ts, first_seen_at, last_seen_at
                )
                SELECT source_team_id, source_channel_id, COALESCE(thread_ts, source_ts),
                       ingested_at, updated_at
                FROM messages
                """
            )

    @staticmethod
    def _validate_pointer(pointer: InboundPointer) -> None:
        required = {
            "event_id": pointer.event_id,
            "source_team_id": pointer.source_team_id,
            "source_channel_id": pointer.source_channel_id,
            "source_ts": pointer.source_ts,
            "event_ts": pointer.event_ts,
            "event_type": pointer.event_type,
        }
        for name, value in required.items():
            if not isinstance(value, str) or not value:
                raise ValueError(f"{name} must be a non-empty string")
        InboxStore._slack_timestamp(pointer.event_ts, "event_ts")

    def _migrate_terminal_receipts(self, table: str, column: str) -> None:
        """Upgrade pre-revision receipt rows without losing their acknowledgement."""
        columns = {row["name"] for row in self._connection.execute(f"PRAGMA table_info({table})")}
        if "revision" in columns:
            return
        legacy = f"{table}_source_level"
        self._connection.execute(f"ALTER TABLE {table} RENAME TO {legacy}")
        self._connection.execute(
            f"""
            CREATE TABLE {table} (
                message_id INTEGER NOT NULL REFERENCES messages(id),
                revision INTEGER NOT NULL,
                {column} REAL NOT NULL,
                PRIMARY KEY(message_id, revision)
            )
            """
        )
        self._connection.execute(
            f"""
            INSERT INTO {table} (message_id, revision, {column})
            SELECT legacy.message_id, messages.revision, legacy.{column}
            FROM {legacy} AS legacy
            JOIN messages ON messages.id = legacy.message_id
            """
        )
        self._connection.execute(f"DROP TABLE {legacy}")

    @staticmethod
    def _slack_timestamp(value: str, name: str) -> Decimal:
        try:
            timestamp = Decimal(value)
        except (InvalidOperation, ValueError) as error:
            raise ValueError(f"{name} must be a numeric Slack timestamp") from error
        if not timestamp.is_finite():
            raise ValueError(f"{name} must be a finite numeric Slack timestamp")
        return timestamp

    @staticmethod
    def _validate_checkpoint_name(name: str) -> None:
        if not isinstance(name, str) or not name:
            raise ValueError("checkpoint name must be a non-empty string")
