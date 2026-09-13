"""Canonical, fenced responsibility records for Director.

This module deliberately does not classify conversations or schedule work.  A
manager records an explicit responsibility, then workers may claim only the
ones the manager has made runnable.  Attention cards are a separate UI view;
their actions use the helpers below while holding the card service's SQLite
transaction so neither record can advance independently.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import sqlite3
import threading
import time
import uuid


RUNNABLE = "runnable"
WAITING_INPUT = "waiting_input"
DEFERRED = "deferred"
CLAIMED = "claimed"
COMPLETED = "completed"
CANCELLED = "cancelled"
STATES = {RUNNABLE, WAITING_INPUT, DEFERRED, CLAIMED, COMPLETED, CANCELLED}


class ResponsibilityError(RuntimeError):
    """A responsibility transition was invalid or its execution fence was lost."""


@dataclass(frozen=True)
class Responsibility:
    id: str
    outcome: str
    next_action: str
    state: str
    conversation_url: str
    current_thread: str | None
    source_message_id: int | None
    source_revision: int | None
    reconsider_at: float | None
    deadline_at: float | None
    version: int
    execution_fence: str | None
    claim_expires_at: float | None
    attention_card_id: str | None
    created_at: float
    updated_at: float


@dataclass(frozen=True)
class ResponsibilityClaim:
    responsibility: Responsibility
    holder: str
    execution_fence: str


@dataclass(frozen=True)
class ResponsibilityHistory:
    version: int
    event: str
    state: str
    conversation_url: str
    current_thread: str | None
    source_message_id: int | None
    source_revision: int | None
    next_action: str
    created_at: float


def initialize_schema(connection: sqlite3.Connection) -> None:
    """Create responsibility tables in a database shared with the card store."""

    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS responsibilities (
            id TEXT PRIMARY KEY,
            outcome TEXT NOT NULL,
            next_action TEXT NOT NULL,
            state TEXT NOT NULL CHECK (state IN ('runnable', 'waiting_input', 'deferred', 'claimed', 'completed', 'cancelled')),
            conversation_url TEXT NOT NULL,
            current_thread TEXT,
            source_message_id INTEGER,
            source_revision INTEGER,
            reconsider_at REAL,
            deadline_at REAL,
            version INTEGER NOT NULL CHECK (version > 0),
            execution_fence TEXT,
            claim_expires_at REAL,
            attention_card_id TEXT,
            created_at REAL NOT NULL,
            updated_at REAL NOT NULL
        )
        """
    )
    columns = {str(row["name"]) for row in connection.execute("PRAGMA table_info(responsibilities)").fetchall()}
    for name, definition in (("claim_expires_at", "REAL"), ("source_message_id", "INTEGER"), ("source_revision", "INTEGER")):
        if name not in columns:
            connection.execute(f"ALTER TABLE responsibilities ADD COLUMN {name} {definition}")
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS responsibility_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            responsibility_id TEXT NOT NULL,
            version INTEGER NOT NULL,
            event TEXT NOT NULL,
            state TEXT NOT NULL,
            conversation_url TEXT NOT NULL,
            current_thread TEXT,
            source_message_id INTEGER,
            source_revision INTEGER,
            next_action TEXT NOT NULL,
            created_at REAL NOT NULL,
            UNIQUE(responsibility_id, version)
        )
        """
    )
    history_columns = {str(row["name"]) for row in connection.execute("PRAGMA table_info(responsibility_history)").fetchall()}
    for name, definition in (("source_message_id", "INTEGER"), ("source_revision", "INTEGER")):
        if name not in history_columns:
            connection.execute(f"ALTER TABLE responsibility_history ADD COLUMN {name} {definition}")
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS responsibility_publications (
            responsibility_id TEXT NOT NULL,
            idempotency_key TEXT NOT NULL,
            execution_fence TEXT NOT NULL,
            summary TEXT NOT NULL,
            state TEXT NOT NULL DEFAULT 'prepared',
            dispatch_started_at REAL,
            created_at REAL NOT NULL,
            PRIMARY KEY (responsibility_id, idempotency_key)
        )
        """
    )
    publication_columns = {str(row["name"]) for row in connection.execute("PRAGMA table_info(responsibility_publications)").fetchall()}
    for name, definition in (("state", "TEXT NOT NULL DEFAULT 'prepared'"), ("dispatch_started_at", "REAL")):
        if name not in publication_columns:
            connection.execute(f"ALTER TABLE responsibility_publications ADD COLUMN {name} {definition}")


class ResponsibilityStore:
    """A small SQLite repository with optimistic worker fences."""

    def __init__(self, path: str | Path) -> None:
        self._lock = threading.RLock()
        self._connection = sqlite3.connect(str(path), isolation_level=None, check_same_thread=False)
        self._connection.row_factory = sqlite3.Row
        self._connection.execute("PRAGMA busy_timeout = 5000")
        initialize_schema(self._connection)

    def close(self) -> None:
        with self._lock:
            self._connection.close()

    def __enter__(self) -> "ResponsibilityStore":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def create(
        self,
        responsibility_id: str,
        outcome: str,
        next_action: str,
        conversation_url: str,
        *,
        current_thread: str | None = None,
        source_message_id: int | None = None,
        source_revision: int | None = None,
        deadline_at: float | None = None,
        state: str = WAITING_INPUT,
        now: float | None = None,
    ) -> Responsibility:
        _validate_fields(responsibility_id, outcome, next_action, conversation_url, state)
        if deadline_at is not None:
            _finite_time(deadline_at, "deadline_at")
        timestamp = time.time() if now is None else _finite_time(now, "now")
        with self._transaction():
            existing = self._connection.execute("SELECT * FROM responsibilities WHERE id = ?", (responsibility_id,)).fetchone()
            if existing is not None:
                current = _responsibility(existing)
                if (
                    current.outcome,
                    current.next_action,
                    current.conversation_url,
                    current.current_thread,
                    current.source_message_id,
                    current.source_revision,
                    current.deadline_at,
                    current.state,
                ) != (outcome, next_action, conversation_url, current_thread, source_message_id, source_revision, deadline_at, state):
                    raise ResponsibilityError("responsibility id was reused for different data")
                return current
            self._connection.execute(
                """
                INSERT INTO responsibilities (
                    id, outcome, next_action, state, conversation_url, current_thread, source_message_id, source_revision,
                    reconsider_at, deadline_at, version, execution_fence, attention_card_id,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, 1, NULL, NULL, ?, ?)
                """,
                (responsibility_id, outcome, next_action, state, conversation_url, current_thread, source_message_id, source_revision, deadline_at, timestamp, timestamp),
            )
            responsibility = self.get(responsibility_id)
            _history(self._connection, responsibility, "created", timestamp)
            return responsibility

    def get(self, responsibility_id: str) -> Responsibility:
        row = self._connection.execute("SELECT * FROM responsibilities WHERE id = ?", (responsibility_id,)).fetchone()
        if row is None:
            raise ResponsibilityError("unknown responsibility")
        return _responsibility(row)

    def list(self, *, runnable_only: bool = False) -> tuple[Responsibility, ...]:
        with self._transaction():
            self._reap_expired_claims(time.time())
            return self._list(runnable_only)

    def history(self, responsibility_id: str) -> tuple[ResponsibilityHistory, ...]:
        self.get(responsibility_id)
        rows = self._connection.execute(
            "SELECT * FROM responsibility_history WHERE responsibility_id = ? ORDER BY version", (responsibility_id,)
        ).fetchall()
        return tuple(
            ResponsibilityHistory(
                version=int(row["version"]), event=str(row["event"]), state=str(row["state"]),
                conversation_url=str(row["conversation_url"]), current_thread=row["current_thread"],
                source_message_id=int(row["source_message_id"]) if row["source_message_id"] is not None else None,
                source_revision=int(row["source_revision"]) if row["source_revision"] is not None else None,
                next_action=str(row["next_action"]), created_at=float(row["created_at"]),
            )
            for row in rows
        )

    def _list(self, runnable_only: bool) -> tuple[Responsibility, ...]:
        query = "SELECT * FROM responsibilities"
        parameters: tuple[object, ...] = ()
        if runnable_only:
            query += " WHERE state = ?"
            parameters = (RUNNABLE,)
        query += " ORDER BY deadline_at IS NULL, deadline_at, updated_at, id"
        return tuple(_responsibility(row) for row in self._connection.execute(query, parameters).fetchall())

    def update(
        self,
        responsibility_id: str,
        *,
        outcome: str | None = None,
        next_action: str | None = None,
        conversation_url: str | None = None,
        current_thread: str | None = None,
        source_message_id: int | None = None,
        source_revision: int | None = None,
        deadline_at: float | None = None,
        state: str | None = None,
        now: float | None = None,
    ) -> Responsibility:
        timestamp = time.time() if now is None else _finite_time(now, "now")
        if state is not None and state not in STATES:
            raise ValueError("invalid responsibility state")
        if state in {CLAIMED, COMPLETED, CANCELLED}:
            raise ResponsibilityError("claimed, completed, and cancelled require their dedicated transitions")
        if deadline_at is not None:
            _finite_time(deadline_at, "deadline_at")
        with self._transaction():
            previous = self.get(responsibility_id)
            if previous.state in {COMPLETED, CANCELLED}:
                raise ResponsibilityError("terminal responsibility cannot be updated")
            target_state = state or previous.state
            if previous.state == CLAIMED and target_state == CLAIMED:
                raise ResponsibilityError("a claimed responsibility must be requeued before its context changes")
            if target_state == DEFERRED and previous.state != DEFERRED:
                raise ResponsibilityError("only a linked Later action can defer a responsibility")
            target = {
                "outcome": outcome if outcome is not None else previous.outcome,
                "next_action": next_action if next_action is not None else previous.next_action,
                "conversation_url": conversation_url if conversation_url is not None else previous.conversation_url,
                "current_thread": current_thread if current_thread is not None else previous.current_thread,
                "source_message_id": source_message_id if source_message_id is not None else previous.source_message_id,
                "source_revision": source_revision if source_revision is not None else previous.source_revision,
                "deadline_at": deadline_at if deadline_at is not None else previous.deadline_at,
                "state": target_state,
            }
            _validate_fields(responsibility_id, target["outcome"], target["next_action"], target["conversation_url"], target_state)
            fence = previous.execution_fence if target_state == CLAIMED else None
            expires_at = previous.claim_expires_at if target_state == CLAIMED else None
            reconsider_at = previous.reconsider_at if target_state == DEFERRED else None
            self._connection.execute(
                """
                UPDATE responsibilities
                SET outcome = ?, next_action = ?, conversation_url = ?, current_thread = ?, source_message_id = ?, source_revision = ?, deadline_at = ?,
                    state = ?, reconsider_at = ?, execution_fence = ?, claim_expires_at = ?, version = version + 1, updated_at = ?
                WHERE id = ? AND version = ?
                """,
                (
                    target["outcome"], target["next_action"], target["conversation_url"], target["current_thread"], target["source_message_id"], target["source_revision"],
                    target["deadline_at"], target_state, reconsider_at, fence, expires_at, timestamp, responsibility_id, previous.version,
                ),
            )
            if target_state == RUNNABLE and previous.state in {WAITING_INPUT, DEFERRED}:
                _retire_linked_card(self._connection, responsibility_id, "continued", timestamp)
            updated = self.get(responsibility_id)
            _history(self._connection, updated, "updated", timestamp)
            return updated

    def resume(self, responsibility_id: str, *, now: float | None = None) -> Responsibility:
        """Explicitly reopen a dropped item for a new user-attention cycle."""

        timestamp = time.time() if now is None else _finite_time(now, "now")
        with self._transaction():
            previous = self.get(responsibility_id)
            if previous.state != CANCELLED:
                raise ResponsibilityError("only a cancelled responsibility can be resumed")
            self._connection.execute(
                """
                UPDATE responsibilities
                SET state = ?, execution_fence = NULL, claim_expires_at = NULL, reconsider_at = NULL,
                    attention_card_id = NULL, version = version + 1, updated_at = ?
                WHERE id = ? AND version = ?
                """,
                (WAITING_INPUT, timestamp, responsibility_id, previous.version),
            )
            resumed = self.get(responsibility_id)
            _history(self._connection, resumed, "resumed", timestamp)
            return resumed

    def cancel(self, responsibility_id: str, *, now: float | None = None) -> Responsibility:
        """Explicit manager cancellation with the same fence revocation as Drop."""

        timestamp = time.time() if now is None else _finite_time(now, "now")
        with self._transaction():
            previous = self.get(responsibility_id)
            if previous.state == CANCELLED:
                return previous
            if previous.state == COMPLETED:
                raise ResponsibilityError("completed responsibility cannot be cancelled")
            self._connection.execute(
                """
                UPDATE responsibilities
                SET state = ?, execution_fence = NULL, claim_expires_at = NULL, reconsider_at = NULL,
                    version = version + 1, updated_at = ?
                WHERE id = ? AND version = ?
                """,
                (CANCELLED, timestamp, responsibility_id, previous.version),
            )
            _retire_linked_card(self._connection, responsibility_id, "cancelled", timestamp)
            cancelled = self.get(responsibility_id)
            _history(self._connection, cancelled, "cancelled", timestamp)
            return cancelled

    def claim(self, responsibility_id: str, holder: str, lease_seconds: float = 900, *, now: float | None = None) -> ResponsibilityClaim:
        if not holder:
            raise ValueError("holder must not be empty")
        timestamp = time.time() if now is None else _finite_time(now, "now")
        if lease_seconds <= 0:
            raise ValueError("lease seconds must be positive")
        with self._transaction():
            self._reap_expired_claims(timestamp)
            previous = self.get(responsibility_id)
            if previous.state != RUNNABLE:
                raise ResponsibilityError("responsibility is not runnable")
            fence = str(uuid.uuid4())
            changed = self._connection.execute(
                """
                UPDATE responsibilities
                SET state = ?, execution_fence = ?, claim_expires_at = ?, version = version + 1, updated_at = ?
                WHERE id = ? AND state = ? AND version = ?
                """,
                (CLAIMED, fence, timestamp + float(lease_seconds), timestamp, responsibility_id, RUNNABLE, previous.version),
            ).rowcount
            if changed != 1:
                raise ResponsibilityError("responsibility changed before it could be claimed")
            claimed = self.get(responsibility_id)
            _history(self._connection, claimed, f"claimed:{holder}", timestamp)
            return ResponsibilityClaim(claimed, holder, fence)

    def execution_gate(self, responsibility_id: str, execution_fence: str, *, now: float | None = None) -> bool:
        timestamp = time.time() if now is None else _finite_time(now, "now")
        row = self._connection.execute(
            "SELECT 1 FROM responsibilities WHERE id = ? AND state = ? AND execution_fence = ? AND claim_expires_at > ?",
            (responsibility_id, CLAIMED, execution_fence, timestamp),
        ).fetchone()
        return row is not None

    def record_publication(
        self, responsibility_id: str, execution_fence: str, idempotency_key: str, summary: str, *, now: float | None = None
    ) -> bool:
        if not idempotency_key or not summary:
            raise ValueError("idempotency key and summary must not be empty")
        timestamp = time.time() if now is None else _finite_time(now, "now")
        with self._transaction():
            existing = self._connection.execute(
                "SELECT execution_fence, summary FROM responsibility_publications WHERE responsibility_id = ? AND idempotency_key = ?",
                (responsibility_id, idempotency_key),
            ).fetchone()
            if existing is not None:
                if existing["execution_fence"] != execution_fence or existing["summary"] != summary:
                    raise ResponsibilityError("publication key was reused for different data")
                return True
            if not self.execution_gate(responsibility_id, execution_fence, now=timestamp):
                return False
            self._connection.execute(
                """
                INSERT INTO responsibility_publications
                    (responsibility_id, idempotency_key, execution_fence, summary, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (responsibility_id, idempotency_key, execution_fence, summary, timestamp),
            )
            return True

    def complete(self, responsibility_id: str, execution_fence: str, *, now: float | None = None) -> Responsibility:
        timestamp = time.time() if now is None else _finite_time(now, "now")
        with self._transaction():
            previous = self.get(responsibility_id)
            if not self.execution_gate(responsibility_id, execution_fence, now=timestamp):
                raise ResponsibilityError("execution fence is no longer valid")
            changed = self._connection.execute(
                """
                UPDATE responsibilities
                SET state = ?, execution_fence = NULL, claim_expires_at = NULL, reconsider_at = NULL, version = version + 1, updated_at = ?
                WHERE id = ? AND state = ? AND execution_fence = ? AND claim_expires_at > ? AND version = ?
                """,
                (COMPLETED, timestamp, responsibility_id, CLAIMED, execution_fence, timestamp, previous.version),
            ).rowcount
            if changed != 1:
                raise ResponsibilityError("responsibility changed before completion")
            _resolve_linked_card(self._connection, responsibility_id, timestamp)
            completed = self.get(responsibility_id)
            _history(self._connection, completed, "completed", timestamp)
            return completed

    def _transaction(self):
        return _Transaction(self._connection, self._lock)

    def _reap_expired_claims(self, now: float) -> None:
        rows = self._connection.execute(
            "SELECT * FROM responsibilities WHERE state = ? AND claim_expires_at <= ?", (CLAIMED, now)
        ).fetchall()
        for row in rows:
            responsibility = _responsibility(row)
            self._connection.execute(
                """
                UPDATE responsibilities
                SET state = ?, execution_fence = NULL, claim_expires_at = NULL, version = version + 1, updated_at = ?
                WHERE id = ? AND version = ?
                """,
                (RUNNABLE, now, responsibility.id, responsibility.version),
            )
            _history(self._connection, self.get(responsibility.id), "claim_expired", now)


def apply_card_action(connection: sqlite3.Connection, card_id: str, action_id: str, revisit_at: float | None, now: float) -> None:
    """Mirror a trusted Later/Drop card choice onto its linked record.

    Caller owns a SQLite transaction which also changes the card itself.
    """

    row = connection.execute(
        "SELECT * FROM responsibilities WHERE attention_card_id = ?", (card_id,)
    ).fetchone()
    if row is None:
        return
    responsibility = _responsibility(row)
    if responsibility.state in {COMPLETED, CANCELLED}:
        return
    if action_id == "director_later":
        state, event = DEFERRED, "card_later"
    elif action_id == "director_drop":
        state, event, revisit_at = CANCELLED, "card_drop", None
    else:
        return
    connection.execute(
        """
        UPDATE responsibilities
        SET state = ?, reconsider_at = ?, execution_fence = NULL, claim_expires_at = NULL, version = version + 1, updated_at = ?
        WHERE id = ? AND version = ?
        """,
        (state, revisit_at, now, responsibility.id, responsibility.version),
    )
    _history(connection, _responsibility_by_id(connection, responsibility.id), event, now)


def resurface_card_attention(connection: sqlite3.Connection, card_id: str, now: float) -> None:
    """Clear the reconsider date without granting an execution right."""

    row = connection.execute("SELECT * FROM responsibilities WHERE attention_card_id = ?", (card_id,)).fetchone()
    if row is None:
        return
    responsibility = _responsibility(row)
    if responsibility.state in {COMPLETED, CANCELLED}:
        return
    connection.execute(
        """
        UPDATE responsibilities
        SET state = ?, reconsider_at = NULL, execution_fence = NULL, claim_expires_at = NULL, version = version + 1, updated_at = ?
        WHERE id = ? AND version = ?
        """,
        (WAITING_INPUT, now, responsibility.id, responsibility.version),
    )
    _history(connection, _responsibility_by_id(connection, responsibility.id), "attention_due", now)


def link_attention_card(
    connection: sqlite3.Connection, responsibility_id: str, card_id: str, now: float, *, expected_version: int | None = None
) -> None:
    """Attach one card and retain that linkage on the canonical record."""

    responsibility = _responsibility_by_id(connection, responsibility_id)
    if expected_version is not None and responsibility.version != expected_version:
        raise ResponsibilityError("responsibility changed before its card could be posted")
    if responsibility.state != WAITING_INPUT:
        raise ResponsibilityError("only a waiting-input responsibility can receive an attention card")
    if responsibility.attention_card_id not in {None, card_id}:
        raise ResponsibilityError("responsibility already has a different attention card")
    if responsibility.attention_card_id == card_id:
        return
    connection.execute(
        """
        UPDATE responsibilities
        SET attention_card_id = ?, version = version + 1, updated_at = ?
        WHERE id = ? AND version = ?
        """,
        (card_id, now, responsibility_id, responsibility.version),
    )
    _history(connection, _responsibility_by_id(connection, responsibility_id), "attention_card_posted", now)


def _resolve_linked_card(connection: sqlite3.Connection, responsibility_id: str, now: float) -> None:
    _retire_linked_card(connection, responsibility_id, "completed", now)


def _retire_linked_card(connection: sqlite3.Connection, responsibility_id: str, resolution_state: str, now: float) -> None:
    if connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'slack_inbox_cards'"
    ).fetchone() is None:
        return
    connection.execute(
        """
        UPDATE slack_inbox_cards
        SET state = 'dropped', resolution_state = ?, revisit_at = NULL,
            version = version + 1, render_state = 'pending',
            resurface_nudge_key = NULL, resurface_nudge_sent_at = NULL, updated_at = ?
        WHERE responsibility_id = ?
          AND (state IN ('pending', 'deferred') OR resolution_state = 'continued')
        """,
        (resolution_state, now, responsibility_id),
    )
    if resolution_state == "continued":
        connection.execute(
            "UPDATE responsibilities SET attention_card_id = NULL WHERE id = ?", (responsibility_id,)
        )


def _history(connection: sqlite3.Connection, responsibility: Responsibility, event: str, now: float) -> None:
    connection.execute(
        """
        INSERT INTO responsibility_history
            (responsibility_id, version, event, state, conversation_url, current_thread, source_message_id, source_revision, next_action, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            responsibility.id, responsibility.version, event, responsibility.state,
            responsibility.conversation_url, responsibility.current_thread, responsibility.source_message_id,
            responsibility.source_revision, responsibility.next_action, now,
        ),
    )


def _responsibility_by_id(connection: sqlite3.Connection, responsibility_id: str) -> Responsibility:
    row = connection.execute("SELECT * FROM responsibilities WHERE id = ?", (responsibility_id,)).fetchone()
    if row is None:
        raise ResponsibilityError("unknown responsibility")
    return _responsibility(row)


def _responsibility(row: sqlite3.Row) -> Responsibility:
    return Responsibility(
        id=str(row["id"]), outcome=str(row["outcome"]), next_action=str(row["next_action"]), state=str(row["state"]),
        conversation_url=str(row["conversation_url"]), current_thread=row["current_thread"],
        source_message_id=int(row["source_message_id"]) if row["source_message_id"] is not None else None,
        source_revision=int(row["source_revision"]) if row["source_revision"] is not None else None,
        reconsider_at=float(row["reconsider_at"]) if row["reconsider_at"] is not None else None,
        deadline_at=float(row["deadline_at"]) if row["deadline_at"] is not None else None,
        version=int(row["version"]), execution_fence=row["execution_fence"],
        claim_expires_at=float(row["claim_expires_at"]) if row["claim_expires_at"] is not None else None,
        attention_card_id=row["attention_card_id"],
        created_at=float(row["created_at"]), updated_at=float(row["updated_at"]),
    )


def _validate_fields(responsibility_id: str, outcome: str, next_action: str, conversation_url: str, state: str) -> None:
    if not all(isinstance(value, str) and value for value in (responsibility_id, outcome, next_action, conversation_url)):
        raise ValueError("responsibility id, outcome, next action, and conversation URL must not be empty")
    if state not in STATES:
        raise ValueError("invalid responsibility state")


def _finite_time(value: float, name: str) -> float:
    if not isinstance(value, (float, int)):
        raise ValueError(f"{name} must be Unix seconds")
    return float(value)


class _Transaction:
    def __init__(self, connection: sqlite3.Connection, lock: threading.RLock) -> None:
        self.connection, self.lock = connection, lock

    def __enter__(self) -> None:
        self.lock.acquire()
        self.connection.execute("BEGIN IMMEDIATE")

    def __exit__(self, exc_type: object, *_: object) -> None:
        try:
            self.connection.execute("ROLLBACK" if exc_type else "COMMIT")
        finally:
            self.lock.release()
