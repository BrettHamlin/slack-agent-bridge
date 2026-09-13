"""Durable reminder scheduling and fenced delivery acknowledgements.

This module never sends a reminder.  A caller claims a due reminder, sends it
with ``ReminderLease.service_idempotency_key``, then calls ``mark_sent``.
Using the same key after an uncertain send lets the transport deduplicate a
retry while the local sent receipt remains durable.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
import math
from pathlib import Path
import secrets
import sqlite3
import threading
import time
from typing import Iterator


@dataclass(frozen=True)
class Reminder:
    id: str
    text: str
    due_at: float
    thread_ts: str | None
    created_at: float

    @property
    def service_idempotency_key(self) -> str:
        return f"reminder:{self.id}"


@dataclass(frozen=True)
class ReminderLease:
    id: str
    text: str
    due_at: float
    thread_ts: str | None
    holder: str
    token: str
    expires_at: float

    @property
    def service_idempotency_key(self) -> str:
        return f"reminder:{self.id}"


class ReminderError(RuntimeError):
    pass


class ReminderConflict(ReminderError):
    """A caller reused an id for a different reminder."""


class ReminderLeaseLost(ReminderError):
    """A delivery acknowledgement did not own a currently valid lease."""


class ReminderStore:
    """SQLite-backed reminders using tables isolated by the ``reminders_`` prefix."""

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

    def __enter__(self) -> "ReminderStore":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def create(
        self,
        reminder_id: str,
        text: str,
        due_at: float,
        *,
        thread_ts: str | None = None,
        now: float | None = None,
    ) -> Reminder:
        """Create a reminder once, or return the identical previously-created one.

        Reusing an id with changed payload is rejected rather than silently
        changing an already-scheduled reminder.
        """
        self._validate_create(reminder_id, text, due_at, thread_ts)
        created_at = time.time() if now is None else now
        with self._transaction():
            row = self._connection.execute(
                "SELECT * FROM reminders_items WHERE id = ?", (reminder_id,)
            ).fetchone()
            if row is not None:
                existing = self._to_reminder(row)
                if (
                    existing.text != text
                    or existing.due_at != float(due_at)
                    or existing.thread_ts != thread_ts
                ):
                    raise ReminderConflict(f"reminder id {reminder_id!r} already has different payload")
                return existing
            self._connection.execute(
                """
                INSERT INTO reminders_items (id, text, due_at, thread_ts, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (reminder_id, text, due_at, thread_ts, created_at),
            )
            return Reminder(reminder_id, text, float(due_at), thread_ts, created_at)

    def list_due(self, *, now: float | None = None) -> tuple[Reminder, ...]:
        """List due reminders without acquiring their delivery leases."""
        current_time = time.time() if now is None else now
        with self._lock:
            rows = self._connection.execute(
                """
                SELECT r.* FROM reminders_items AS r
                WHERE r.due_at <= ?
                AND NOT EXISTS (
                    SELECT 1 FROM reminders_sent_receipts AS s WHERE s.reminder_id = r.id
                )
                ORDER BY r.due_at, r.id
                """,
                (current_time,),
            ).fetchall()
            return tuple(self._to_reminder(row) for row in rows)

    def claim_due(
        self,
        holder: str,
        lease_seconds: float,
        limit: int = 1,
        *,
        now: float | None = None,
    ) -> tuple[ReminderLease, ...]:
        """Atomically claim due, unsent reminders whose lease is available."""
        if not isinstance(holder, str) or not holder:
            raise ValueError("holder must be a non-empty string")
        if (
            not isinstance(lease_seconds, (int, float))
            or not math.isfinite(lease_seconds)
            or lease_seconds <= 0
        ):
            raise ValueError("lease_seconds must be a finite positive number")
        if not isinstance(limit, int) or limit <= 0:
            raise ValueError("limit must be positive")
        claimed_at = time.time() if now is None else now
        expires_at = claimed_at + float(lease_seconds)
        with self._transaction():
            rows = self._connection.execute(
                """
                SELECT r.* FROM reminders_items AS r
                WHERE r.due_at <= ?
                AND NOT EXISTS (
                    SELECT 1 FROM reminders_sent_receipts AS s WHERE s.reminder_id = r.id
                )
                AND (r.lease_expires_at IS NULL OR r.lease_expires_at <= ?)
                ORDER BY r.due_at, r.id
                LIMIT ?
                """,
                (claimed_at, claimed_at, limit),
            ).fetchall()
            leases: list[ReminderLease] = []
            for row in rows:
                token = secrets.token_urlsafe(24)
                self._connection.execute(
                    """
                    UPDATE reminders_items
                    SET lease_holder = ?, lease_token = ?, lease_expires_at = ?
                    WHERE id = ?
                    """,
                    (holder, token, expires_at, row["id"]),
                )
                leases.append(
                    ReminderLease(
                        id=str(row["id"]),
                        text=str(row["text"]),
                        due_at=float(row["due_at"]),
                        thread_ts=row["thread_ts"],
                        holder=holder,
                        token=token,
                        expires_at=expires_at,
                    )
                )
            return tuple(leases)

    def mark_sent(self, lease: ReminderLease, *, now: float | None = None) -> None:
        """Persist the sent receipt if and only if ``lease`` still owns the reminder."""
        sent_at = time.time() if now is None else now
        with self._transaction():
            row = self._connection.execute(
                "SELECT * FROM reminders_items WHERE id = ?", (lease.id,)
            ).fetchone()
            if (
                row is None
                or row["lease_holder"] != lease.holder
                or row["lease_token"] != lease.token
                or row["lease_expires_at"] is None
                or float(row["lease_expires_at"]) <= sent_at
            ):
                raise ReminderLeaseLost(f"reminder lease is no longer valid for {lease.id!r}")
            self._connection.execute(
                """
                INSERT INTO reminders_sent_receipts (reminder_id, sent_at, service_idempotency_key)
                VALUES (?, ?, ?)
                ON CONFLICT(reminder_id) DO NOTHING
                """,
                (lease.id, sent_at, lease.service_idempotency_key),
            )
            self._connection.execute(
                """
                UPDATE reminders_items
                SET lease_holder = NULL, lease_token = NULL, lease_expires_at = NULL
                WHERE id = ?
                """,
                (lease.id,),
            )

    def get_sent_at(self, reminder_id: str) -> float | None:
        """Return the local sent receipt time, if the reminder was acknowledged."""
        self._validate_id(reminder_id)
        with self._lock:
            row = self._connection.execute(
                "SELECT sent_at FROM reminders_sent_receipts WHERE reminder_id = ?", (reminder_id,)
            ).fetchone()
            return None if row is None else float(row["sent_at"])

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
                CREATE TABLE IF NOT EXISTS reminders_items (
                    id TEXT PRIMARY KEY,
                    text TEXT NOT NULL,
                    due_at REAL NOT NULL,
                    thread_ts TEXT,
                    created_at REAL NOT NULL,
                    lease_holder TEXT,
                    lease_token TEXT,
                    lease_expires_at REAL
                );

                CREATE TABLE IF NOT EXISTS reminders_sent_receipts (
                    reminder_id TEXT PRIMARY KEY REFERENCES reminders_items(id),
                    sent_at REAL NOT NULL,
                    service_idempotency_key TEXT NOT NULL UNIQUE
                );
                """
            )

    @staticmethod
    def _to_reminder(row: sqlite3.Row) -> Reminder:
        return Reminder(
            id=str(row["id"]),
            text=str(row["text"]),
            due_at=float(row["due_at"]),
            thread_ts=row["thread_ts"],
            created_at=float(row["created_at"]),
        )

    @staticmethod
    def _validate_id(reminder_id: str) -> None:
        if not isinstance(reminder_id, str) or not reminder_id:
            raise ValueError("reminder id must be a non-empty string")

    @classmethod
    def _validate_create(cls, reminder_id: str, text: str, due_at: float, thread_ts: str | None) -> None:
        cls._validate_id(reminder_id)
        if not isinstance(text, str) or not text:
            raise ValueError("reminder text must be a non-empty string")
        if not isinstance(due_at, (int, float)) or not math.isfinite(due_at):
            raise ValueError("due_at must be a finite timestamp")
        if thread_ts is not None and (not isinstance(thread_ts, str) or not thread_ts):
            raise ValueError("thread_ts must be a non-empty string when provided")
