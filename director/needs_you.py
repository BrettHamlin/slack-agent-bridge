"""Durable owner-review items and their private Slack App Home projection.

Owner-review items deliberately have no execution authority.  They record a
completed agent result that the configured owner still needs to inspect; a
checkbox, snooze, or restore must never claim, resume, cancel, or complete a
responsibility.
"""
from __future__ import annotations

from dataclasses import dataclass
import json
import math
import hashlib
from pathlib import Path
import sqlite3
import threading
import time
import secrets
from urllib.parse import urlparse
import uuid
from typing import Any, Mapping


ACTIVE = "active"
SNOOZED = "snoozed"
DONE = "done"
STATES = {ACTIVE, SNOOZED, DONE}
MAX_DONE = 25
MAX_ACTIONABLE = 18


class NeedsYouError(RuntimeError):
    """A needs-you record could not be safely changed."""


@dataclass(frozen=True)
class NeedsYouTarget:
    team_id: str
    owner_user_id: str
    source_channel_id: str
    workspace_domain: str
    action_url: str | None = None
    http_bind_host: str = "127.0.0.1"
    http_port: int = 8080


@dataclass(frozen=True)
class NeedsYouItem:
    id: str
    idempotency_key: str
    team_id: str
    owner_user_id: str
    source_channel_id: str
    root: str
    conversation_url: str
    title: str
    detail: str
    state: str
    version: int
    snoozed_until: float | None
    completed_at: float | None
    created_at: float
    updated_at: float


def needs_you_target(config: Mapping[str, Any]) -> NeedsYouTarget | None:
    """Return the explicitly enabled private App Home projection target."""
    raw = config.get("needs_you")
    if raw is None:
        return None
    if not isinstance(raw, Mapping) or type(raw.get("enabled")) is not bool:
        raise ValueError("needs_you.enabled must be a boolean")
    if not raw["enabled"]:
        return None
    required = ("team_id", "owner_user_id", "channel_id", "workspace_domain")
    if any(not isinstance(config.get(name), str) or not config[name] for name in required):
        raise ValueError("needs_you requires a complete Slack identity")
    action_url = raw.get("action_url")
    if action_url is not None:
        if not isinstance(action_url, str):
            raise ValueError("needs_you.action_url must be a string")
        parsed = urlparse(action_url)
        if (parsed.scheme != "https" or not parsed.netloc or parsed.path not in {"", "/"}
                or parsed.query or parsed.fragment or parsed.username or parsed.password):
            raise ValueError("needs_you.action_url must be an HTTPS origin without credentials or a path")
        action_url = action_url.rstrip("/")
    bind_host = raw.get("http_bind_host", "127.0.0.1")
    port = raw.get("http_port", 8080)
    if bind_host != "127.0.0.1" or type(port) is not int or not 1 <= port <= 65535:
        raise ValueError("needs_you HTTP server must bind loopback on a valid port")
    return NeedsYouTarget(
        str(config["team_id"]), str(config["owner_user_id"]),
        str(config["channel_id"]), str(config["workspace_domain"]), action_url, bind_host, port,
    )


class NeedsYouStore:
    """SQLite state for the owner-review lifecycle, separate from work state."""

    def __init__(self, path: str | Path, target: NeedsYouTarget | None) -> None:
        self.target = target
        self._lock = threading.RLock()
        self._connection = sqlite3.connect(str(path), isolation_level=None, check_same_thread=False)
        self._connection.row_factory = sqlite3.Row
        self._connection.execute("PRAGMA foreign_keys = ON")
        self._connection.execute("PRAGMA busy_timeout = 5000")
        self._initialize()

    @property
    def configured(self) -> bool:
        return self.target is not None

    def close(self) -> None:
        with self._lock:
            self._connection.close()

    def _initialize(self) -> None:
        self._connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS needs_you_items (
                id TEXT PRIMARY KEY,
                idempotency_key TEXT NOT NULL UNIQUE,
                team_id TEXT NOT NULL,
                owner_user_id TEXT NOT NULL,
                source_channel_id TEXT NOT NULL,
                root TEXT NOT NULL,
                conversation_url TEXT NOT NULL,
                title TEXT NOT NULL,
                detail TEXT NOT NULL,
                state TEXT NOT NULL CHECK (state IN ('active', 'snoozed', 'done')),
                version INTEGER NOT NULL CHECK (version > 0),
                snoozed_until REAL,
                completed_at REAL,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL
            );
            CREATE INDEX IF NOT EXISTS needs_you_items_state_updated
                ON needs_you_items(team_id, owner_user_id, state, updated_at DESC);
            CREATE TABLE IF NOT EXISTS needs_you_actions (
                action_ts TEXT PRIMARY KEY,
                item_id TEXT NOT NULL,
                action TEXT NOT NULL,
                changed INTEGER NOT NULL,
                created_at REAL NOT NULL,
                FOREIGN KEY(item_id) REFERENCES needs_you_items(id)
            );
            CREATE TABLE IF NOT EXISTS needs_you_action_links (
                token_hash TEXT PRIMARY KEY,
                item_id TEXT NOT NULL,
                expected_version INTEGER NOT NULL,
                operation TEXT NOT NULL CHECK (operation IN ('snooze', 'bring_back')),
                created_at REAL NOT NULL,
                expires_at REAL NOT NULL,
                used_at REAL,
                FOREIGN KEY(item_id) REFERENCES needs_you_items(id)
            );
            CREATE TABLE IF NOT EXISTS needs_you_oidc_flows (
                state_hash TEXT PRIMARY KEY,
                nonce TEXT NOT NULL,
                link_token_hash TEXT NOT NULL,
                created_at REAL NOT NULL,
                expires_at REAL NOT NULL,
                FOREIGN KEY(link_token_hash) REFERENCES needs_you_action_links(token_hash)
            );
            CREATE TABLE IF NOT EXISTS needs_you_web_sessions (
                token_hash TEXT PRIMARY KEY,
                csrf_hash TEXT NOT NULL,
                link_token_hash TEXT NOT NULL,
                team_id TEXT NOT NULL,
                user_id TEXT NOT NULL,
                created_at REAL NOT NULL,
                expires_at REAL NOT NULL,
                used_at REAL,
                FOREIGN KEY(link_token_hash) REFERENCES needs_you_action_links(token_hash)
            );
            """
        )

    def record_completed_result(
        self, *, idempotency_key: str, root: str, conversation_url: str,
        title: str, detail: str,
    ) -> NeedsYouItem | None:
        """Create the one owner-review record after an answer is confirmed sent."""
        if self.target is None:
            return None
        _validate_item_input(idempotency_key, root, conversation_url, title, detail)
        target = self.target
        with self._lock:
            existing = self._connection.execute(
                "SELECT * FROM needs_you_items WHERE idempotency_key=?", (idempotency_key,)
            ).fetchone()
            if existing is not None:
                return _item(existing)
            now = time.time()
            item_id = str(uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"slack-agent-bridge:needs-you:{target.team_id}:{target.owner_user_id}:{idempotency_key}",
            ))
            self._connection.execute(
                """INSERT INTO needs_you_items
                   (id,idempotency_key,team_id,owner_user_id,source_channel_id,root,conversation_url,
                    title,detail,state,version,snoozed_until,completed_at,created_at,updated_at)
                   VALUES (?,?,?,?,?,?,?,?,?, 'active',1,NULL,NULL,?,?)""",
                (item_id, idempotency_key, target.team_id, target.owner_user_id,
                 target.source_channel_id, root, conversation_url, title, detail, now, now),
            )
            return self.get(item_id)

    def get(self, item_id: str) -> NeedsYouItem:
        with self._lock:
            row = self._connection.execute("SELECT * FROM needs_you_items WHERE id=?", (item_id,)).fetchone()
        if row is None:
            raise NeedsYouError("unknown needs-you item")
        return _item(row)

    def list_for_home(self, page: int = 0) -> tuple[list[NeedsYouItem], list[NeedsYouItem], list[NeedsYouItem], int, int, bool]:
        """Return one explicit App Home page without silently dropping items."""
        if self.target is None:
            return [], [], [], 0, 0, False
        if not isinstance(page, int) or page < 0:
            raise NeedsYouError("invalid needs-you page")
        target = self.target
        with self._lock:
            active_all = self._connection.execute(
                """SELECT * FROM needs_you_items WHERE team_id=? AND owner_user_id=? AND source_channel_id=? AND state='active'
                   ORDER BY updated_at ASC, id ASC""", (target.team_id, target.owner_user_id, target.source_channel_id)
            ).fetchall()
            snoozed_all = self._connection.execute(
                """SELECT * FROM needs_you_items WHERE team_id=? AND owner_user_id=? AND source_channel_id=? AND state='snoozed'
                   ORDER BY snoozed_until ASC, id ASC""", (target.team_id, target.owner_user_id, target.source_channel_id)
            ).fetchall()
            done = self._connection.execute(
                """SELECT * FROM needs_you_items WHERE team_id=? AND owner_user_id=? AND source_channel_id=? AND state='done'
                   ORDER BY completed_at DESC, id DESC LIMIT ?""", (target.team_id, target.owner_user_id, target.source_channel_id, MAX_DONE)
            ).fetchall()
        active, snoozed = list(map(_item, active_all)), list(map(_item, snoozed_all))
        start = page * MAX_ACTIONABLE
        slice_rows = [(ACTIVE, item) for item in active] + [(SNOOZED, item) for item in snoozed]
        visible = slice_rows[start:start + MAX_ACTIONABLE]
        shown_active = [item for state, item in visible if state == ACTIVE]
        shown_snoozed = [item for state, item in visible if state == SNOOZED]
        return shown_active, shown_snoozed, list(map(_item, done)), len(active), len(snoozed), start + MAX_ACTIONABLE < len(slice_rows)

    def set_done(self, *, item_id: str, expected_version: int, done: bool, action_ts: str) -> tuple[NeedsYouItem, bool]:
        """Set a checkbox's requested state with replay and stale-view fences."""
        if self.target is None or not _valid_action_ts(action_ts):
            raise NeedsYouError("invalid needs-you action")
        with self._lock:
            self._connection.execute("BEGIN IMMEDIATE")
            try:
                receipt = self._connection.execute(
                    "SELECT item_id,action,changed FROM needs_you_actions WHERE action_ts=?", (action_ts,)
                ).fetchone()
                if receipt is not None:
                    if receipt["item_id"] != item_id or receipt["action"] != ("done" if done else "restore"):
                        self._connection.execute("ROLLBACK")
                        raise NeedsYouError("needs-you action replay mismatch")
                    item = self.get(item_id)
                    self._connection.execute("COMMIT")
                    return item, bool(receipt["changed"])
                row = self._connection.execute("SELECT * FROM needs_you_items WHERE id=?", (item_id,)).fetchone()
                if row is None or not _owns(row, self.target) or int(row["version"]) != expected_version:
                    self._connection.execute("ROLLBACK")
                    raise NeedsYouError("stale or out-of-scope needs-you item")
                next_state = DONE if done else ACTIVE
                now = time.time()
                changed = False
                if row["state"] != next_state:
                    changed = self._connection.execute(
                        """UPDATE needs_you_items
                           SET state=?,version=version+1,snoozed_until=NULL,
                               completed_at=?,updated_at=?
                           WHERE id=? AND version=?""",
                        (next_state, now if done else None, now, item_id, expected_version),
                    ).rowcount == 1
                self._connection.execute(
                    "INSERT INTO needs_you_actions(action_ts,item_id,action,changed,created_at) VALUES (?,?,?,?,?)",
                    (action_ts, item_id, "done" if done else "restore", int(changed), now),
                )
                item = self.get(item_id)
                self._connection.execute("COMMIT")
                return item, changed
            except Exception:
                if self._connection.in_transaction:
                    self._connection.execute("ROLLBACK")
                raise

    def snooze(self, *, item_id: str, expected_version: int, until: float) -> NeedsYouItem:
        """Move an owner-review item aside; it never resumes agent execution."""
        if (self.target is None or not isinstance(until, (int, float)) or not math.isfinite(until)
                or until <= time.time()):
            raise NeedsYouError("invalid snooze time")
        return self._set_state(item_id, expected_version, SNOOZED, float(until))

    def resurface_due(self, *, now: float | None = None) -> int:
        """Promote due owner reviews without resuming an agent responsibility."""
        if self.target is None:
            return 0
        current = time.time() if now is None else now
        target = self.target
        with self._lock:
            return self._connection.execute(
                """UPDATE needs_you_items
                   SET state='active',version=version+1,snoozed_until=NULL,updated_at=?
                   WHERE team_id=? AND owner_user_id=? AND source_channel_id=?
                     AND state='snoozed' AND snoozed_until <= ?""",
                (current, target.team_id, target.owner_user_id, target.source_channel_id, current),
            ).rowcount

    def bring_back(self, *, item_id: str, expected_version: int) -> NeedsYouItem:
        return self._set_state(item_id, expected_version, ACTIVE, None)

    def create_action_link(self, item: NeedsYouItem, operation: str) -> str | None:
        """Create an opaque navigation link; it cannot itself authorize a change."""
        if self.target is None or self.target.action_url is None or operation not in {"snooze", "bring_back"}:
            return None
        if not _owns_item(item, self.target):
            raise NeedsYouError("out-of-scope needs-you link")
        token = secrets.token_urlsafe(32)
        now = time.time()
        with self._lock:
            self._connection.execute(
                """INSERT INTO needs_you_action_links
                   (token_hash,item_id,expected_version,operation,created_at,expires_at,used_at)
                   VALUES (?,?,?,?,?,?,NULL)""",
                # This is only opaque navigation to an authenticated
                # confirmation page. Version fencing and owner sign-in, not
                # a short URL expiry, protect the mutation.
                (_hash_token(token), item.id, item.version, operation, now, now + 365 * 86400),
            )
        return f"{self.target.action_url}/needs-you/action/{token}?op={operation}"

    def begin_oidc_flow(self, link_token: str, operation: str) -> tuple[str, str]:
        """Create a short-lived OAuth state bound to one displayed link."""
        if self.target is None or operation not in {"snooze", "bring_back"}:
            raise NeedsYouError("invalid needs-you action link")
        now = time.time()
        link_hash = _hash_token(link_token)
        with self._lock:
            link = self._connection.execute(
                """SELECT * FROM needs_you_action_links WHERE token_hash=? AND operation=?
                   AND used_at IS NULL""", (link_hash, operation)
            ).fetchone()
            if link is None:
                raise NeedsYouError("expired needs-you action link")
            state, nonce = secrets.token_urlsafe(32), secrets.token_urlsafe(32)
            self._connection.execute(
                "INSERT INTO needs_you_oidc_flows(state_hash,nonce,link_token_hash,created_at,expires_at) VALUES (?,?,?,?,?)",
                (_hash_token(state), nonce, link_hash, now, now + 600),
            )
        return state, nonce

    def complete_oidc_flow(self, state: str, *, team_id: str, user_id: str, nonce: str) -> tuple[str, str]:
        """Turn a verified Slack identity into one opaque web session."""
        if self.target is None or team_id != self.target.team_id or user_id != self.target.owner_user_id:
            raise NeedsYouError("needs-you sign-in identity mismatch")
        now = time.time()
        with self._lock:
            self._connection.execute("BEGIN IMMEDIATE")
            try:
                flow = self._connection.execute(
                    "SELECT * FROM needs_you_oidc_flows WHERE state_hash=?", (_hash_token(state),)
                ).fetchone()
                if flow is None or flow["expires_at"] <= now or not secrets.compare_digest(str(flow["nonce"]), nonce):
                    self._connection.execute("ROLLBACK")
                    raise NeedsYouError("expired needs-you sign-in")
                token, csrf = secrets.token_urlsafe(32), secrets.token_urlsafe(32)
                self._connection.execute(
                    """INSERT INTO needs_you_web_sessions
                       (token_hash,csrf_hash,link_token_hash,team_id,user_id,created_at,expires_at,used_at)
                       VALUES (?,?,?,?,?,?,?,NULL)""",
                    (_hash_token(token), _hash_token(csrf), flow["link_token_hash"], team_id, user_id, now, now + 600),
                )
                self._connection.execute("DELETE FROM needs_you_oidc_flows WHERE state_hash=?", (_hash_token(state),))
                self._connection.execute("COMMIT")
                return token, csrf
            except Exception:
                if self._connection.in_transaction:
                    self._connection.execute("ROLLBACK")
                raise

    def web_session(self, token: str) -> tuple[NeedsYouItem, str] | None:
        """Read a still-valid owner-only confirmation session."""
        now = time.time()
        with self._lock:
            row = self._connection.execute(
                """SELECT s.*,l.item_id,l.expected_version,l.operation FROM needs_you_web_sessions AS s
                   JOIN needs_you_action_links AS l ON l.token_hash=s.link_token_hash
                   WHERE s.token_hash=? AND s.used_at IS NULL AND s.expires_at>? AND l.used_at IS NULL""",
                (_hash_token(token), now),
            ).fetchone()
            if row is None or self.target is None or row["team_id"] != self.target.team_id or row["user_id"] != self.target.owner_user_id:
                return None
            item = self._connection.execute("SELECT * FROM needs_you_items WHERE id=?", (row["item_id"],)).fetchone()
        if item is None or not _owns(item, self.target) or int(item["version"]) != int(row["expected_version"]):
            return None
        return _item(item), str(row["operation"])

    def apply_web_action(self, token: str, csrf: str, *, snooze_seconds: int | None = None) -> NeedsYouItem:
        """Consume one authenticated page confirmation and update one current item."""
        now = time.time()
        with self._lock:
            self._connection.execute("BEGIN IMMEDIATE")
            try:
                row = self._connection.execute(
                    """SELECT s.*,l.item_id,l.expected_version,l.operation FROM needs_you_web_sessions AS s
                       JOIN needs_you_action_links AS l ON l.token_hash=s.link_token_hash
                       WHERE s.token_hash=? AND s.used_at IS NULL AND s.expires_at>? AND l.used_at IS NULL""",
                    (_hash_token(token), now),
                ).fetchone()
                if (row is None or self.target is None or row["team_id"] != self.target.team_id
                        or row["user_id"] != self.target.owner_user_id
                        or not secrets.compare_digest(str(row["csrf_hash"]), _hash_token(csrf))):
                    self._connection.execute("ROLLBACK")
                    raise NeedsYouError("invalid needs-you confirmation")
                item = self._connection.execute("SELECT * FROM needs_you_items WHERE id=?", (row["item_id"],)).fetchone()
                if item is None or not _owns(item, self.target) or int(item["version"]) != int(row["expected_version"]):
                    self._connection.execute("ROLLBACK")
                    raise NeedsYouError("stale needs-you confirmation")
                operation = row["operation"]
                if operation == "snooze":
                    if snooze_seconds not in {86400, 259200, 604800}:
                        self._connection.execute("ROLLBACK")
                        raise NeedsYouError("invalid needs-you snooze")
                    state, due = SNOOZED, now + snooze_seconds
                else:
                    state, due = ACTIVE, None
                self._connection.execute(
                    """UPDATE needs_you_items SET state=?,version=version+1,snoozed_until=?,completed_at=NULL,updated_at=?
                       WHERE id=? AND version=?""", (state, due, now, item["id"], item["version"]),
                )
                self._connection.execute("UPDATE needs_you_action_links SET used_at=? WHERE token_hash=?", (now, row["link_token_hash"]))
                self._connection.execute("UPDATE needs_you_web_sessions SET used_at=? WHERE token_hash=?", (now, _hash_token(token)))
                updated = self._connection.execute("SELECT * FROM needs_you_items WHERE id=?", (item["id"],)).fetchone()
                self._connection.execute("COMMIT")
                return _item(updated)
            except Exception:
                if self._connection.in_transaction:
                    self._connection.execute("ROLLBACK")
                raise

    def _set_state(self, item_id: str, expected_version: int, state: str, until: float | None) -> NeedsYouItem:
        with self._lock:
            row = self._connection.execute("SELECT * FROM needs_you_items WHERE id=?", (item_id,)).fetchone()
            if row is None or not _owns(row, self.target) or int(row["version"]) != expected_version:
                raise NeedsYouError("stale or out-of-scope needs-you item")
            now = time.time()
            changed = self._connection.execute(
                """UPDATE needs_you_items
                   SET state=?, version=version+1, snoozed_until=?, completed_at=NULL, updated_at=?
                   WHERE id=? AND version=?""", (state, until, now, item_id, expected_version)
            ).rowcount
            if changed != 1:
                raise NeedsYouError("needs-you item changed concurrently")
            return self.get(item_id)


class NeedsYouHome:
    """App Home renderer and interaction gate; network publishing stays receiver-owned."""

    def __init__(self, web_client: object, path: str | Path, config: Mapping[str, Any]) -> None:
        self.target = needs_you_target(config)
        self.store = NeedsYouStore(path, self.target)
        self._web = web_client
        self._lock = threading.RLock()
        self._publish_requested = False
        self._page = 0

    @property
    def configured(self) -> bool:
        return self.target is not None

    def close(self) -> None:
        self.store.close()

    def private_metadata(self) -> str:
        if self.target is None:
            return ""
        return json.dumps({
            "kind": "needs_you", "team_id": self.target.team_id,
            "owner_user_id": self.target.owner_user_id,
            "source_channel_id": self.target.source_channel_id,
        }, sort_keys=True, separators=(",", ":"))

    def queue_open(self, payload: Mapping[str, Any]) -> bool:
        if not _valid_home_open(payload, self.target):
            return False
        with self._lock:
            self._publish_requested = True
        return True

    def request_publish(self) -> None:
        """Request a fresh Home projection after a local state change."""
        with self._lock:
            self._publish_requested = True

    def publish_if_requested(self) -> bool:
        with self._lock:
            if not self._publish_requested:
                return False
            self._publish_requested = False
        if self.target is None:
            return False
        try:
            self._web.views_publish(user_id=self.target.owner_user_id, view=self.render())
        except Exception:
            with self._lock:
                self._publish_requested = True
            raise
        return True

    def render(self) -> dict[str, object]:
        active, snoozed, done, active_count, snoozed_count, has_more = self.store.list_for_home(self._page)
        blocks: list[dict[str, object]] = [
            {"type": "header", "text": {"type": "plain_text", "text": "Needs you", "emoji": True}},
            {"type": "context", "elements": [{"type": "mrkdwn", "text": _summary(active_count, snoozed_count)}]},
        ]
        for item in active:
            blocks.extend(self._item_blocks(item))
        blocks.append({"type": "divider"})
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": f"*Snoozed · {snoozed_count}*"}})
        for item in snoozed:
            blocks.extend(self._item_blocks(item))
        blocks.append({"type": "divider"})
        blocks.extend([
            {"type": "section", "text": {"type": "mrkdwn", "text": "*Done*"}},
            {"type": "context", "elements": [{"type": "mrkdwn", "text": "Most recent 25 · Uncheck an item to bring it back."}]},
        ])
        for item in done:
            blocks.extend(self._item_blocks(item))
        if self._page or has_more:
            elements: list[dict[str, object]] = []
            if self._page:
                elements.append({"type": "button", "text": {"type": "plain_text", "text": "Previous items"}, "action_id": "needs_you_page", "value": "previous"})
            if has_more:
                elements.append({"type": "button", "text": {"type": "plain_text", "text": "More items"}, "action_id": "needs_you_page", "value": "next"})
            blocks.append({"type": "actions", "block_id": "needs_you_page", "elements": elements})
        if len(blocks) > 100:
            raise NeedsYouError("needs-you App Home exceeds Slack block limit")
        return {"type": "home", "private_metadata": self.private_metadata(), "blocks": blocks}

    def _item_blocks(self, item: NeedsYouItem) -> list[dict[str, object]]:
        done = item.state == DONE
        if done:
            # Slack's option-object limit includes escaped mrkdwn and the
            # visual strike markers, not just the original title length.
            title_text = f"~{_escape_mrkdwn(item.title)[:73]}~"
            text = {"type": "mrkdwn", "text": title_text}
        else:
            text = {"type": "plain_text", "text": item.title, "emoji": True}
        option = {"text": text, "value": f"{item.id}:{item.version}"}
        checkbox: dict[str, object] = {
            "type": "checkboxes", "action_id": "needs_you_set_done", "options": [option],
        }
        if done:
            checkbox["initial_options"] = [option]
        if item.state == SNOOZED:
            timing, operation, label = f"Back {_format_time(item.snoozed_until)}", "bring_back", "Bring back"
        elif done:
            timing, operation, label = f"Completed {_format_time(item.completed_at)}", None, None
        else:
            timing, operation, label = item.detail, "snooze", "Snooze"
        context = f"{_escape_mrkdwn(timing)} · <{item.conversation_url}|Open conversation>"
        if operation is not None:
            link = self.store.create_action_link(item, operation)
            if link is not None:
                context += f" · <{link}|{label}>"
        return [
            {"type": "actions", "block_id": f"needs_you:{item.id}:{item.version}", "elements": [checkbox]},
            {"type": "context", "elements": [{"type": "mrkdwn", "text": context}]},
        ]

    def handle_interaction(self, payload: Mapping[str, Any]) -> bool:
        parsed = _parse_home_interaction(payload, self.target, self.private_metadata())
        if parsed is None:
            return False
        kind, values = parsed
        if kind == "checkbox":
            item_id, version, done, action_ts = values
            try:
                self.store.set_done(item_id=item_id, expected_version=version, done=done, action_ts=action_ts)
            except NeedsYouError:
                # A stale App Home view is normal after another device or a
                # due-time transition.  Acknowledge it and refresh, rather
                # than asking Slack to replay the same stale action forever.
                with self._lock:
                    self._publish_requested = True
                return False
        elif kind == "page":
            self._page = max(0, self._page + (1 if values == "next" else -1))
        else:
            return False
        with self._lock:
            self._publish_requested = True
        return True

    def resurface_due(self) -> bool:
        changed = self.store.resurface_due()
        if changed:
            with self._lock:
                self._publish_requested = True
        return bool(changed)


class NeedsYouHomeWorker:
    """Publish Home updates away from the receiver's Socket Mode tick."""

    def __init__(self, home: NeedsYouHome, on_error=None) -> None:
        self.home = home
        self.on_error = on_error or (lambda _value: None)
        self._wake = threading.Event()
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="director-needs-you-home", daemon=True)

    def start(self) -> None:
        self._thread.start()

    def wake(self) -> None:
        self._wake.set()

    def close(self) -> None:
        self._stop.set()
        self._wake.set()
        self._thread.join(timeout=5)

    def _run(self) -> None:
        retry_delay = 0.25
        while not self._stop.is_set():
            self._wake.wait(timeout=30)
            self._wake.clear()
            if self._stop.is_set():
                return
            try:
                if self.home.publish_if_requested():
                    self.on_error("")
                retry_delay = 0.25
            except Exception as error:
                self.on_error(type(error).__name__)
                # The Home keeps the request dirty. Back off before retrying
                # so a Slack outage cannot tie up this receiver-owned thread.
                self._stop.wait(retry_delay)
                retry_delay = min(retry_delay * 2, 30)
                self._wake.set()


def _summary(ready: int, later: int) -> str:
    return f"{ready} ready for review · {later} snoozed"


def _format_time(value: float | None) -> str:
    if value is None:
        return "recently"
    return time.strftime("%b %-d at %-I:%M %p", time.localtime(value))


def _parse_home_interaction(payload: Mapping[str, Any], target: NeedsYouTarget | None, metadata: str) -> tuple[str, object] | None:
    if target is None or payload.get("type") != "block_actions":
        return None
    team, user, view = payload.get("team"), payload.get("user"), payload.get("view")
    if not isinstance(team, Mapping) or not isinstance(user, Mapping) or not isinstance(view, Mapping):
        return None
    if team.get("id") != target.team_id or user.get("id") != target.owner_user_id or view.get("private_metadata") != metadata:
        return None
    actions = payload.get("actions")
    if not isinstance(actions, list) or len(actions) != 1 or not isinstance(actions[0], Mapping):
        return None
    action = actions[0]
    if action.get("action_id") == "needs_you_page":
        value = action.get("value")
        return ("page", value) if value in {"next", "previous"} else None
    if action.get("action_id") != "needs_you_set_done" or not isinstance(action.get("action_ts"), str):
        return None
    action_ts = str(action["action_ts"])
    if not _valid_action_ts(action_ts):
        return None
    done = bool(action.get("selected_options"))
    block_id = action.get("block_id")
    if not isinstance(block_id, str):
        return None
    parts = block_id.split(":")
    if len(parts) != 3 or parts[0] != "needs_you" or not parts[1] or not parts[2].isdigit():
        return None
    return "checkbox", (parts[1], int(parts[2]), done, action_ts)


def _valid_home_open(payload: Mapping[str, Any], target: NeedsYouTarget | None) -> bool:
    if target is None or payload.get("type") != "event_callback":
        return False
    event = payload.get("event")
    return bool(
        isinstance(event, Mapping) and event.get("type") == "app_home_opened"
        and payload.get("team_id") == target.team_id and event.get("user") == target.owner_user_id
    )


def _validate_item_input(idempotency_key: str, root: str, conversation_url: str, title: str, detail: str) -> None:
    if not all(isinstance(value, str) and value.strip() for value in (idempotency_key, root, conversation_url, title)):
        raise NeedsYouError("needs-you item fields are required")
    if not isinstance(detail, str) or len(title) > 75 or len(detail) > 300:
        raise NeedsYouError("needs-you item text is invalid")
    if not conversation_url.startswith("https://") or "\n" in conversation_url:
        raise NeedsYouError("needs-you conversation URL is invalid")
    if any("\n" in value for value in (title, detail)):
        raise NeedsYouError("needs-you item text must be single-line")


def _valid_action_ts(value: str) -> bool:
    return bool(value) and len(value) <= 64 and all(char.isdigit() or char == "." for char in value)


def _escape_mrkdwn(value: str) -> str:
    return value.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _hash_token(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _owns_item(item: NeedsYouItem, target: NeedsYouTarget) -> bool:
    return (item.team_id == target.team_id and item.owner_user_id == target.owner_user_id
            and item.source_channel_id == target.source_channel_id)


def _owns(row: sqlite3.Row, target: NeedsYouTarget | None) -> bool:
    return bool(target and row["team_id"] == target.team_id and row["owner_user_id"] == target.owner_user_id and row["source_channel_id"] == target.source_channel_id)


def _item(row: sqlite3.Row) -> NeedsYouItem:
    return NeedsYouItem(
        id=str(row["id"]), idempotency_key=str(row["idempotency_key"]), team_id=str(row["team_id"]),
        owner_user_id=str(row["owner_user_id"]), source_channel_id=str(row["source_channel_id"]),
        root=str(row["root"]), conversation_url=str(row["conversation_url"]), title=str(row["title"]),
        detail=str(row["detail"]), state=str(row["state"]), version=int(row["version"]),
        snoozed_until=float(row["snoozed_until"]) if row["snoozed_until"] is not None else None,
        completed_at=float(row["completed_at"]) if row["completed_at"] is not None else None,
        created_at=float(row["created_at"]), updated_at=float(row["updated_at"]),
    )
