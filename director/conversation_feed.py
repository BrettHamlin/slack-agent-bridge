"""Durable, separate-channel conversation index for Director.

The feed is deliberately a projection of confirmed Director replies.  It does
not ingest Slack events, invoke an agent, or participate in the reply delivery
transaction.  A receiver maintenance tick publishes and reconciles it after a
reply has already reached its original conversation.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import sqlite3
import threading
import time
import uuid
from typing import Any, Mapping


class ConversationFeedError(RuntimeError):
    """The conversation feed could not safely advance its projection."""


@dataclass(frozen=True)
class FeedTarget:
    channel_id: str
    workspace_domain: str
    owner_user_id: str
    bot_user_id: str
    source_channel_id: str


_EMOJI_POOL = (
    "📌", "🧭", "🔐", "🚀", "🧪", "📋", "✉️", "🛠️", "📊", "🧩",
    "🗂️", "🔎", "💡", "🧱", "🎯", "🧵", "🗺️", "⚙️", "📱", "🌱",
)


def feed_target(config: Mapping[str, Any]) -> FeedTarget | None:
    """Return the configured target or ``None`` while the feature is disabled."""

    raw = config.get("conversation_feed")
    if raw is None:
        return None
    if not isinstance(raw, Mapping) or type(raw.get("enabled")) is not bool:
        raise ValueError("conversation_feed.enabled must be a boolean")
    if not raw["enabled"]:
        return None
    channel_id = raw.get("channel_id")
    if not isinstance(channel_id, str) or not channel_id:
        raise ValueError("conversation_feed.channel_id is required when enabled")
    source = config.get("channel_id")
    if not isinstance(source, str) or channel_id == source:
        raise ValueError("conversation feed must use a different channel from its source")
    required = ("workspace_domain", "owner_user_id", "bot_user_id")
    if any(not isinstance(config.get(name), str) or not config[name] for name in required):
        raise ValueError("conversation feed requires a complete Slack identity")
    return FeedTarget(channel_id, config["workspace_domain"], config["owner_user_id"], config["bot_user_id"], source)


class ConversationFeed:
    """One durable per-root card projection in a separate private channel."""

    def __init__(self, web_client: object, path: str | Path, config: Mapping[str, Any]) -> None:
        self.target = feed_target(config)
        self._web = web_client
        self._available = True
        self._lock = threading.RLock()
        self._connection = sqlite3.connect(str(path), isolation_level=None, check_same_thread=False)
        self._connection.row_factory = sqlite3.Row
        self._connection.execute("PRAGMA busy_timeout = 5000")
        self._reserved_approval_ids: set[str] = set()
        self._initialize()

    @property
    def enabled(self) -> bool:
        return self.target is not None and self._available

    @property
    def configured(self) -> bool:
        return self.target is not None

    def disable(self) -> None:
        """Fence feed writes after target verification fails; source work continues."""
        self._available = False

    def close(self) -> None:
        with self._lock:
            self._connection.close()

    def record_reply(
        self,
        *,
        root: str,
        title: str | None,
        emoji: str | None,
        preview: str | None,
        outgoing_key: str,
        outgoing_ts: str,
    ) -> bool:
        """Durably queue a confirmed original reply for later feed publication.

        The first projection needs model-authored display metadata.  Later
        replies retain the recorded title and emoji, while their model-authored
        preview replaces the older answer.  Missing metadata never delays or
        rejects the original reply.
        """

        if not self.configured:
            return False
        _validate_ts(root, "root")
        _validate_ts(outgoing_ts, "outgoing timestamp")
        if not isinstance(outgoing_key, str) or not outgoing_key:
            raise ValueError("outgoing key is required")
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM conversation_feed_sessions WHERE root = ?", (root,)
            ).fetchone()
            if row is None:
                clean_title, clean_emoji, clean_preview = _validate_metadata(title, emoji, preview)
                clean_emoji = self._allocate_emoji(clean_emoji, root, float(outgoing_ts))
                self._connection.execute(
                    """
                    INSERT INTO conversation_feed_sessions (
                        root, source_channel_id, conversation_url, title, emoji,
                        visibility, latest_preview, latest_outgoing_key, latest_outgoing_ts,
                        latest_activity_at, desired_generation, settled_generation,
                        current_card_ts, feed_channel_id, post_state, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, 'visible', ?, ?, ?, ?, 1, 0, NULL, ?, 'prepared', ?, ?)
                    """,
                    (root, self.target.source_channel_id, self._conversation_url(root), clean_title,
                     clean_emoji, clean_preview, outgoing_key, outgoing_ts, float(outgoing_ts), self.target.channel_id, time.time(), time.time()),
                )
                return True
            # A user-hidden card stays hidden when recovery replays its last
            # confirmed delivery. A stale Guardian retrying marker must not
            # turn that old receipt into fresh feed activity; a genuinely
            # newer confirmed reply still follows the update path below.
            if float(outgoing_ts) <= float(row["latest_activity_at"]) and (
                row['visibility'] == 'hidden' or row['approval_state'] is None
            ):
                return False
            if row['feed_channel_id'] != self.target.channel_id:
                raise ConversationFeedError('conversation feed target changed; migrate existing sessions explicitly')
            clean_preview = _validate_preview(preview)
            self._connection.execute(
                """
                UPDATE conversation_feed_sessions
                SET visibility = 'visible', latest_preview = ?, latest_outgoing_key = ?,
                    latest_outgoing_ts = ?, latest_activity_at = ?, desired_generation = desired_generation + 1,
                    post_state = CASE WHEN post_state IN ('posted', 'rejected') THEN 'prepared' ELSE post_state END,
                    retry_at = CASE WHEN post_state = 'rejected' THEN 0 ELSE retry_at END,
                    approval_id = NULL, approval_job_key = NULL, approval_state = NULL, approval_text = NULL,
                    approval_expires_at = NULL, updated_at = ?
                WHERE root = ?
                """,
                (clean_preview, outgoing_key, outgoing_ts, float(outgoing_ts), time.time(), root),
            )
            return True

    def record_guardian_pending(
        self,
        *,
        root: str,
        job_key: str,
        title: str,
        emoji: str,
        preview: str,
        proposal_text: str,
        expires_at: float,
    ) -> str | None:
        """Project one terminally-pending native approval into the feed.

        The opaque returned token is solely a feed-card binding.  It is never a
        native review, authority, fingerprint, or payload handle.
        """
        if not self.configured:
            return None
        _validate_ts(root, "root")
        if not isinstance(job_key, str) or not job_key or not isinstance(proposal_text, str) or not proposal_text:
            raise ValueError("guardian feed projection requires a job key and exact proposed reply")
        if not isinstance(expires_at, (int, float)) or expires_at <= 0:
            raise ValueError("guardian feed projection requires an expiry")
        clean_title, clean_emoji, clean_preview = _validate_metadata(title, emoji, preview)
        with self._lock:
            row = self._connection.execute("SELECT * FROM conversation_feed_sessions WHERE root=?", (root,)).fetchone()
            now = time.time()
            if row is not None and row['approval_job_key'] == job_key:
                # Projection recovery and ACP event replay must never turn a
                # terminal/hidden approval back into a fresh clickable card.
                return str(row['approval_id']) if row['approval_id'] else None
            token = uuid.uuid4().hex
            if row is None:
                clean_emoji = self._allocate_emoji(clean_emoji, root, float(root))
                self._connection.execute(
                    """INSERT INTO conversation_feed_sessions (
                       root,source_channel_id,conversation_url,title,emoji,visibility,latest_preview,
                       latest_outgoing_key,latest_outgoing_ts,latest_activity_at,desired_generation,
                       settled_generation,current_card_ts,feed_channel_id,post_state,
                       approval_id,approval_job_key,approval_state,approval_text,approval_expires_at,
                       created_at,updated_at)
                       VALUES (?,?,?,?,?,'visible',?,'',?,?,1,0,NULL,?,'prepared',?,?,?,?,?,?,?)""",
                    (root, self.target.source_channel_id, self._conversation_url(root), clean_title, clean_emoji,
                     clean_preview, root, now, self.target.channel_id, token, job_key, 'pending',
                     proposal_text, float(expires_at), now, now),
                )
            else:
                if row['feed_channel_id'] != self.target.channel_id:
                    raise ConversationFeedError('conversation feed target changed; migrate existing sessions explicitly')
                # A pending approval is fresh activity, so it moves this
                # session to the newest end while preserving its identity.
                self._connection.execute(
                    """UPDATE conversation_feed_sessions
                       SET visibility='visible', latest_preview=?, latest_activity_at=?, desired_generation=desired_generation+1,
                           post_state=CASE WHEN post_state IN ('posted','rejected') THEN 'prepared' ELSE post_state END,
                           retry_at=CASE WHEN post_state='rejected' THEN 0 ELSE retry_at END,
                           approval_id=?,approval_job_key=?,approval_state='pending',approval_text=?,
                           approval_expires_at=?,updated_at=? WHERE root=?""",
                    (clean_preview, now, token, job_key, proposal_text, float(expires_at), now, root),
                )
            return token

    def reserve_guardian_click(self, approval_id: str) -> bool:
        """Fast in-memory callback reservation; durable validation is loop-owned."""
        if not isinstance(approval_id, str) or len(approval_id) != 32:
            return False
        with self._lock:
            if approval_id in self._reserved_approval_ids:
                return False
            self._reserved_approval_ids.add(approval_id)
            return True

    def begin_guardian_approval(self, approval_id: str, card_ts: str) -> str | None:
        """Durably fence a current pending card before native approval is submitted."""
        _validate_ts(card_ts, "feed card timestamp")
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM conversation_feed_sessions WHERE approval_id=?", (approval_id,)
            ).fetchone()
            if (row is None or row['visibility'] != 'visible' or row['approval_state'] != 'pending'
                    or row['current_card_ts'] != card_ts or row['feed_channel_id'] != self.target.channel_id):
                self._reserved_approval_ids.discard(approval_id)
                return None
            changed = self._connection.execute(
                """UPDATE conversation_feed_sessions SET approval_state='retrying', desired_generation=desired_generation+1,
                   post_state=CASE WHEN post_state IN ('posted','rejected') THEN 'prepared' ELSE post_state END,
                   retry_at=CASE WHEN post_state='rejected' THEN 0 ELSE retry_at END,updated_at=?
                   WHERE root=? AND approval_id=? AND approval_state='pending' AND current_card_ts=?""",
                (time.time(), row['root'], approval_id, card_ts),
            ).rowcount
            self._reserved_approval_ids.discard(approval_id)
            return str(row['approval_job_key']) if changed == 1 else None

    def set_guardian_approval_state(self, job_key: str, state: str) -> bool:
        """Render a terminal approval state without changing conversation order."""
        if state not in {'failed', 'expired', 'restart_inactive', 'inactive'}:
            raise ValueError('invalid guardian approval feed state')
        with self._lock:
            changed = self._connection.execute(
                """UPDATE conversation_feed_sessions SET approval_state=?,desired_generation=desired_generation+1,
                   post_state=CASE WHEN post_state IN ('posted','rejected') THEN 'prepared' ELSE post_state END,
                   retry_at=CASE WHEN post_state='rejected' THEN 0 ELSE retry_at END,updated_at=?
                   WHERE approval_job_key=? AND approval_state IN ('pending','retrying')""",
                (state, time.time(), job_key),
            ).rowcount
            return changed == 1

    def import_record(self, payload: Mapping[str, Any]) -> bool:
        """Queue a curator-supplied historical session after outbox validation."""

        if not self.enabled:
            raise ConversationFeedError("conversation feed is disabled")
        required = ("root", "title", "emoji", "preview", "outgoing_key", "outgoing_ts")
        if not isinstance(payload, Mapping) or set(payload) != set(required):
            raise ValueError("feed import requires exactly root, title, emoji, preview, outgoing_key, outgoing_ts")
        root = payload["root"]
        outgoing_key = payload["outgoing_key"]
        outgoing_ts = payload["outgoing_ts"]
        if not isinstance(root, str) or not isinstance(outgoing_key, str) or not isinstance(outgoing_ts, str):
            raise ValueError("feed import root and outgoing receipt must be strings")
        _validate_ts(root, "root")
        _validate_ts(outgoing_ts, "outgoing timestamp")
        with self._lock:
            receipt = self._connection.execute(
                """SELECT thread_ts, slack_ts FROM slack_outbox
                   WHERE idempotency_key = ? AND state = 'sent'""",
                (outgoing_key,),
            ).fetchone()
        if receipt is None or receipt["thread_ts"] != root or receipt["slack_ts"] != outgoing_ts:
            raise ConversationFeedError("feed import must reference an exact confirmed original reply")
        return self.record_reply(
            root=root, title=payload["title"], emoji=payload["emoji"], preview=payload["preview"],
            outgoing_key=outgoing_key, outgoing_ts=outgoing_ts,
        )

    def hide(self, root: str) -> bool:
        """Remove this feed projection only; the original Slack thread is untouched."""

        if not self.enabled:
            raise ConversationFeedError("conversation feed is disabled")
        _validate_ts(root, "root")
        with self._lock:
            self._connection.execute('BEGIN IMMEDIATE')
            row = self._connection.execute(
                "SELECT current_card_ts, visibility FROM conversation_feed_sessions WHERE root = ?", (root,)
            ).fetchone()
            if row is None:
                self._connection.execute('COMMIT')
                return False
            try:
                self._connection.execute(
                    "UPDATE conversation_feed_sessions SET visibility = 'hidden', updated_at = ? WHERE root = ?",
                    (time.time(), root),
                )
                if row["current_card_ts"]:
                    self._queue_delete(root, str(row["current_card_ts"]))
                self._connection.execute('COMMIT')
                return row["visibility"] != "hidden"
            except Exception:
                self._connection.execute('ROLLBACK')
                raise

    def reconcile(self, *, limit: int = 12) -> dict[str, int]:
        """Advance queued posts and deletions; never call an agent or source intake."""

        if not self.enabled:
            return {"posted": 0, "deleted": 0, "uncertain": 0}
        if not isinstance(limit, int) or limit < 1:
            raise ValueError("limit must be positive")
        counts = {"posted": 0, "deleted": 0, "uncertain": 0}
        with self._lock:
            deletions = self._connection.execute(
                """SELECT root, card_ts, state FROM conversation_feed_deletions
                   WHERE state != 'deleted' AND retry_at <= ?
                   ORDER BY created_at LIMIT ?""",
                (time.time(), limit),
            ).fetchall()
        mutations = 0
        for item in deletions:
            result = self._reconcile_delete(str(item["root"]), str(item["card_ts"]))
            counts[result] += 1
            mutations += int(item['state'] == 'prepared')
        # Uncertain deletes first perform a read-only evidence check. They do
        # not consume a mutation slot or starve a newer session card.
        remaining = max(0, limit - mutations)
        if not remaining:
            return counts
        with self._lock:
            sessions = self._connection.execute(
                """SELECT * FROM conversation_feed_sessions
                   WHERE ((visibility = 'visible' AND desired_generation > settled_generation)
                      OR (visibility = 'hidden' AND post_state IN ('dispatching','uncertain')))
                   AND retry_at <= ?
                   ORDER BY CASE post_state WHEN 'prepared' THEN 0 ELSE 1 END, latest_activity_at ASC LIMIT ?""",
                (time.time(), remaining),
            ).fetchall()
        for session in sessions:
            result = self._reconcile_post(dict(session))
            counts[result] += 1
        return counts

    def has_pending_work(self) -> bool:
        if not self.enabled:
            return False
        with self._lock:
            return bool(self._connection.execute(
                "SELECT 1 FROM conversation_feed_deletions WHERE state != 'deleted' LIMIT 1"
            ).fetchone() or self._connection.execute(
                """SELECT 1 FROM conversation_feed_sessions
                   WHERE visibility = 'visible' AND desired_generation > settled_generation LIMIT 1"""
            ).fetchone())

    def _reconcile_post(self, session: Mapping[str, Any]) -> str:
        root = str(session["root"])
        with self._lock:
            current = self._connection.execute(
                "SELECT * FROM conversation_feed_sessions WHERE root = ?", (root,)
            ).fetchone()
            if current is None:
                return "deleted"
            if current['feed_channel_id'] != self.target.channel_id:
                return "uncertain"
            state = str(current["post_state"])
            if state in {"dispatching", "uncertain"}:
                generation = current['inflight_generation']
                client_msg_id = current['inflight_client_msg_id']
                if not isinstance(generation, int) or not isinstance(client_msg_id, str):
                    return "uncertain"
                # History is network I/O; do not hold the state lock while an
                # original reply attempts to queue a newer feed generation.
                reconcile_only = True
            elif current['visibility'] != 'visible':
                return 'deleted'
            else:
                reconcile_only = False
                generation = int(current["desired_generation"])
                client_msg_id = _client_message_id(self.target, root, generation)
            if reconcile_only:
                pass
            else:
                if state == 'rejected':
                    return 'uncertain'
                changed = self._connection.execute(
                    """UPDATE conversation_feed_sessions
                       SET post_state = 'dispatching', inflight_generation = ?, inflight_client_msg_id = ?,
                           inflight_title = title, inflight_emoji = emoji, inflight_preview = latest_preview,
                           inflight_approval_id = approval_id, inflight_approval_state = approval_state,
                           inflight_approval_text = approval_text, inflight_approval_expires_at = approval_expires_at,
                           updated_at = ?
                       WHERE root = ? AND desired_generation = ? AND post_state = 'prepared'""",
                    (generation, client_msg_id, time.time(), root, generation),
                ).rowcount
                if changed != 1:
                    return "uncertain"
                current = self._connection.execute(
                    "SELECT * FROM conversation_feed_sessions WHERE root = ?", (root,)
                ).fetchone()
        if reconcile_only:
            found = self._find_client_message(client_msg_id)
            if found is None:
                return "uncertain"
            return self._settle_post(root, generation, found)
        render = dict(current)
        render['title'] = current['inflight_title']
        render['emoji'] = current['inflight_emoji']
        render['latest_preview'] = current['inflight_preview']
        render['approval_id'] = current['inflight_approval_id']
        render['approval_state'] = current['inflight_approval_state']
        render['approval_text'] = current['inflight_approval_text']
        render['approval_expires_at'] = current['inflight_approval_expires_at']
        try:
            response = _response_data(getattr(self._web, "chat_postMessage")(
                channel=self.target.channel_id,
                text=_fallback_text(render),
                blocks=_blocks(render),
                client_msg_id=client_msg_id,
            ))
            if _known_rejection(response):
                with self._lock:
                    self._connection.execute("UPDATE conversation_feed_sessions SET post_state = ?, retry_at = ?, updated_at = ? WHERE root = ? AND inflight_generation = ?", ('prepared' if _rate_limited(response) else 'rejected', time.time() + _retry_after(response), time.time(), root, generation))
                return 'uncertain'
            if not _valid_target_response(response, self.target.channel_id):
                raise ConversationFeedError('feed post target was not confirmed')
            ts = _successful_ts(response)
            if ts is None:
                raise ConversationFeedError("feed post was not confirmed")
        except Exception as error:
            with self._lock:
                self._connection.execute(
                    "UPDATE conversation_feed_sessions SET post_state = ?, retry_at = ?, updated_at = ? WHERE root = ? AND inflight_generation = ?",
                    ('prepared' if _rate_limited(error) else 'rejected' if _known_rejection(error) else 'uncertain', time.time() + _retry_after(error) if _rate_limited(error) else 0, time.time(), root, generation),
                )
            return "uncertain"
        return self._settle_post(root, generation, ts)

    def _settle_post(self, root: str, generation: int, card_ts: str) -> str:
        _validate_ts(card_ts, "feed card timestamp")
        with self._lock:
            self._connection.execute("BEGIN IMMEDIATE")
            try:
                row = self._connection.execute(
                    "SELECT * FROM conversation_feed_sessions WHERE root = ?", (root,)
                ).fetchone()
                if row is None:
                    self._queue_delete(root, card_ts)
                    self._connection.execute("COMMIT")
                    return "deleted"
                if row["visibility"] != "visible" or int(row["desired_generation"]) != generation:
                    self._queue_delete(root, card_ts)
                    if row['visibility'] == 'visible':
                        self._connection.execute(
                            """UPDATE conversation_feed_sessions
                               SET post_state = 'prepared', inflight_generation = NULL, inflight_client_msg_id = NULL,
                                   inflight_title = NULL, inflight_emoji = NULL, inflight_preview = NULL, inflight_approval_id = NULL, inflight_approval_state = NULL, inflight_approval_text = NULL, inflight_approval_expires_at = NULL, updated_at = ?
                               WHERE root = ? AND inflight_generation = ?""",
                            (time.time(), root, generation),
                        )
                    else:
                        self._connection.execute(
                            """UPDATE conversation_feed_sessions
                               SET post_state = 'posted', settled_generation = ?, inflight_generation = NULL,
                                   inflight_client_msg_id = NULL, inflight_title = NULL, inflight_emoji = NULL,
                                   inflight_preview = NULL, inflight_approval_id = NULL, inflight_approval_state = NULL,
                                   inflight_approval_text = NULL, inflight_approval_expires_at = NULL, updated_at = ?
                               WHERE root = ? AND inflight_generation = ?""",
                            (generation, time.time(), root, generation),
                        )
                    self._connection.execute("COMMIT")
                    return "deleted"
                previous = row["current_card_ts"]
                self._connection.execute(
                    """UPDATE conversation_feed_sessions
                       SET current_card_ts = ?, settled_generation = ?, post_state = 'posted', updated_at = ?
                       WHERE root = ? AND desired_generation = ?""",
                    (card_ts, generation, time.time(), root, generation),
                )
                if previous and previous != card_ts:
                    self._queue_delete(root, str(previous))
                self._connection.execute("COMMIT")
                return "posted"
            except Exception:
                if self._connection.in_transaction:
                    self._connection.execute("ROLLBACK")
                raise

    def _reconcile_delete(self, root: str, card_ts: str) -> str:
        with self._lock:
            row = self._connection.execute(
                "SELECT state, feed_channel_id FROM conversation_feed_deletions WHERE root = ? AND card_ts = ?", (root, card_ts)
            ).fetchone()
            if row is None or row["state"] == "deleted":
                return "deleted"
            if row['feed_channel_id'] != self.target.channel_id:
                return 'uncertain'
            evidence_needed = row['state'] in ('dispatching', 'uncertain')
        if evidence_needed:
            exists = self._feed_message_exists(card_ts)
            if exists is None:
                return 'uncertain'
            with self._lock:
                if not exists:
                    self._connection.execute("UPDATE conversation_feed_deletions SET state = 'deleted', updated_at = ? WHERE root = ? AND card_ts = ?", (time.time(), root, card_ts))
                    self._connection.execute("UPDATE conversation_feed_sessions SET current_card_ts = NULL, updated_at = ? WHERE root = ? AND current_card_ts = ? AND visibility = 'hidden'", (time.time(), root, card_ts))
                    return 'deleted'
                self._connection.execute("UPDATE conversation_feed_deletions SET state = 'prepared', updated_at = ? WHERE root = ? AND card_ts = ?", (time.time(), root, card_ts))
                return 'uncertain'
        with self._lock:
            self._connection.execute(
                "UPDATE conversation_feed_deletions SET state = 'dispatching', updated_at = ? WHERE root = ? AND card_ts = ?",
                (time.time(), root, card_ts),
            )
        try:
            raw_response = getattr(self._web, "chat_delete")(channel=self.target.channel_id, ts=card_ts)
            response = _response_data(raw_response)
            if _known_rejection(response):
                # Slack confirmed that no deletion was accepted. Preserve the
                # operation and defer it; a later item can still publish.
                with self._lock:
                    self._connection.execute(
                        """UPDATE conversation_feed_deletions
                           SET state = 'prepared', retry_at = ?, updated_at = ?
                           WHERE root = ? AND card_ts = ?""",
                        (time.time() + _retry_after(raw_response), time.time(), root, card_ts),
                    )
                return "uncertain"
            if not _valid_target_response(response, self.target.channel_id):
                raise ConversationFeedError("feed deletion was not confirmed")
        except Exception as error:
            if _rate_limited(error):
                # A 429 is definitely not an accepted delete. Honor the
                # server's delay durably, including across a restart.
                with self._lock:
                    self._connection.execute(
                        """UPDATE conversation_feed_deletions
                           SET state = 'prepared', retry_at = ?, updated_at = ?
                           WHERE root = ? AND card_ts = ?""",
                        (time.time() + _retry_after(error), time.time(), root, card_ts),
                    )
                return "uncertain"
            # A delete has no idempotency field.  Confirm absence on later
            # maintenance instead of blindly retrying after an ambiguous call.
            exists = self._feed_message_exists(card_ts)
            if exists is None or exists:
                with self._lock:
                    self._connection.execute(
                        "UPDATE conversation_feed_deletions SET state = 'uncertain', retry_at = 0, updated_at = ? WHERE root = ? AND card_ts = ?",
                        (time.time(), root, card_ts),
                    )
                return "uncertain"
        with self._lock:
            self._connection.execute(
            "UPDATE conversation_feed_deletions SET state = 'deleted', retry_at = 0, updated_at = ? WHERE root = ? AND card_ts = ?",
                (time.time(), root, card_ts),
            )
            self._connection.execute(
                "UPDATE conversation_feed_sessions SET current_card_ts = NULL, updated_at = ? WHERE root = ? AND current_card_ts = ? AND visibility = 'hidden'",
                (time.time(), root, card_ts),
            )
        return "deleted"

    def _queue_delete(self, root: str, card_ts: str) -> None:
        self._connection.execute(
            """INSERT INTO conversation_feed_deletions (root, card_ts, feed_channel_id, state, retry_at, created_at, updated_at)
               VALUES (?, ?, ?, 'prepared', 0, ?, ?) ON CONFLICT(root, card_ts) DO NOTHING""",
            (root, card_ts, self.target.channel_id, time.time(), time.time()),
        )

    def _find_client_message(self, client_msg_id: str) -> str | None:
        cursor = None
        seen = set()
        while True:
            kwargs = {'channel': self.target.channel_id, 'limit': 200}
            if cursor:
                kwargs['cursor'] = cursor
            response = _response_data(getattr(self._web, "conversations_history")(**kwargs))
            if not _valid_target_response(response, self.target.channel_id):
                return None
            messages = response.get("messages")
            if not isinstance(messages, list):
                return None
            for message in messages:
                if not isinstance(message, Mapping) or message.get("client_msg_id") != client_msg_id:
                    continue
                if message.get("user") == self.target.bot_user_id:
                    value = message.get("ts")
                    return value if isinstance(value, str) and _is_ts(value) else None
            metadata = response.get('response_metadata')
            cursor = metadata.get('next_cursor') if isinstance(metadata, Mapping) else None
            if not isinstance(cursor, str) or not cursor:
                return None
            if cursor in seen:
                return None
            seen.add(cursor)

    def _feed_message_exists(self, card_ts: str) -> bool | None:
        try:
            response = _response_data(getattr(self._web, "conversations_history")(
                channel=self.target.channel_id, oldest=card_ts, latest=card_ts, inclusive=True, limit=1
            ))
        except Exception:
            # An unavailable history read is not proof of a successful delete.
            return None
        if not _valid_target_response(response, self.target.channel_id):
            return None
        messages = response.get("messages")
        if not isinstance(messages, list):
            return None
        if any(not isinstance(item, Mapping) or not isinstance(item.get('ts'), str) for item in messages):
            return None
        return any(item.get("ts") == card_ts for item in messages)

    def _allocate_emoji(self, requested: str, root: str, activity_at: float) -> str:
        recent = {
            str(row["emoji"])
            for row in self._connection.execute(
                "SELECT emoji FROM conversation_feed_sessions WHERE root != ? AND latest_activity_at >= ?",
                (root, activity_at - 7 * 24 * 60 * 60),
            ).fetchall()
        }
        if requested not in recent:
            return requested
        offset = int(hashlib.sha256(root.encode()).hexdigest(), 16) % len(_EMOJI_POOL)
        for index in range(len(_EMOJI_POOL)):
            candidate = _EMOJI_POOL[(offset + index) % len(_EMOJI_POOL)]
            if candidate not in recent:
                return candidate
        # Keep the visible marker unique even after the curated palette is
        # exhausted. This is persisted once, so later replies never reshuffle.
        suffix = 1
        while True:
            candidate = f"{requested}{suffix}\N{COMBINING ENCLOSING KEYCAP}"
            if candidate not in recent:
                return candidate
            suffix += 1

    def _conversation_url(self, root: str) -> str:
        return (
            f"https://{self.target.workspace_domain}/archives/{self.target.source_channel_id}/"
            f"p{root.replace('.', '')}?thread_ts={root}&cid={self.target.source_channel_id}"
        )

    def _initialize(self) -> None:
        with self._lock:
            self._connection.execute(
                """CREATE TABLE IF NOT EXISTS conversation_feed_sessions (
                    root TEXT PRIMARY KEY, source_channel_id TEXT NOT NULL, conversation_url TEXT NOT NULL,
                    title TEXT NOT NULL, emoji TEXT NOT NULL, visibility TEXT NOT NULL CHECK (visibility IN ('visible','hidden')),
                    latest_preview TEXT NOT NULL, latest_outgoing_key TEXT NOT NULL, latest_outgoing_ts TEXT NOT NULL,
                    latest_activity_at REAL NOT NULL, desired_generation INTEGER NOT NULL, settled_generation INTEGER NOT NULL,
                    current_card_ts TEXT, feed_channel_id TEXT NOT NULL,
                    post_state TEXT NOT NULL CHECK (post_state IN ('prepared','dispatching','uncertain','posted','rejected')),
                    inflight_generation INTEGER, inflight_client_msg_id TEXT,
                    inflight_title TEXT, inflight_emoji TEXT, inflight_preview TEXT,
                    approval_id TEXT, approval_job_key TEXT, approval_state TEXT, approval_text TEXT,
                    approval_expires_at REAL, inflight_approval_id TEXT, inflight_approval_state TEXT,
                    inflight_approval_text TEXT, inflight_approval_expires_at REAL,
                    retry_at REAL NOT NULL DEFAULT 0, created_at REAL NOT NULL, updated_at REAL NOT NULL
                )"""
            )
            columns = {str(row['name']) for row in self._connection.execute('PRAGMA table_info(conversation_feed_sessions)').fetchall()}
            for name, definition in (
                ('feed_channel_id', "TEXT NOT NULL DEFAULT ''"), ('inflight_generation', 'INTEGER'),
                ('inflight_client_msg_id', 'TEXT'), ('inflight_title', 'TEXT'),
                ('inflight_emoji', 'TEXT'), ('inflight_preview', 'TEXT'),
                ('approval_id', 'TEXT'), ('approval_job_key', 'TEXT'), ('approval_state', 'TEXT'),
                ('approval_text', 'TEXT'), ('approval_expires_at', 'REAL'),
                ('inflight_approval_id', 'TEXT'), ('inflight_approval_state', 'TEXT'),
                ('inflight_approval_text', 'TEXT'), ('inflight_approval_expires_at', 'REAL'),
                ('retry_at', 'REAL NOT NULL DEFAULT 0'),
            ):
                if name not in columns:
                    self._connection.execute(f'ALTER TABLE conversation_feed_sessions ADD COLUMN {name} {definition}')
            self._connection.execute(
                """CREATE TABLE IF NOT EXISTS conversation_feed_deletions (
                    root TEXT NOT NULL, card_ts TEXT NOT NULL,
                    feed_channel_id TEXT NOT NULL DEFAULT '',
                    state TEXT NOT NULL CHECK (state IN ('prepared','dispatching','uncertain','deleted')),
                    retry_at REAL NOT NULL DEFAULT 0, created_at REAL NOT NULL, updated_at REAL NOT NULL,
                    PRIMARY KEY(root, card_ts)
                )"""
            )
            delete_columns = {str(row['name']) for row in self._connection.execute('PRAGMA table_info(conversation_feed_deletions)').fetchall()}
            if 'feed_channel_id' not in delete_columns:
                self._connection.execute("ALTER TABLE conversation_feed_deletions ADD COLUMN feed_channel_id TEXT NOT NULL DEFAULT ''")
            if 'retry_at' not in delete_columns:
                self._connection.execute("ALTER TABLE conversation_feed_deletions ADD COLUMN retry_at REAL NOT NULL DEFAULT 0")


def _validate_metadata(title: object, emoji: object, preview: object) -> tuple[str, str, str]:
    return _validate_title(title), _validate_emoji(emoji), _validate_preview(preview)


def _validate_title(value: object) -> str:
    if not isinstance(value, str) or not (clean := value.strip()) or "\n" in clean or len(clean) > 100:
        raise ValueError("conversation title must be one non-empty line of at most 100 characters")
    return clean


def _validate_emoji(value: object) -> str:
    if not isinstance(value, str) or not (clean := value.strip()) or "\n" in clean or len(clean) > 32:
        raise ValueError("conversation emoji must be one non-empty short line")
    return clean


def _validate_preview(value: object) -> str:
    if not isinstance(value, str) or not (clean := value.strip()) or len(clean) > 600:
        raise ValueError("conversation preview must be non-empty and at most 600 characters")
    return clean


def _validate_ts(value: object, label: str) -> None:
    if not isinstance(value, str) or not _is_ts(value):
        raise ValueError(f"{label} must be a Slack timestamp")


def _is_ts(value: str) -> bool:
    try:
        return bool(value) and float(value) >= 0
    except ValueError:
        return False


def _client_message_id(target: FeedTarget, root: str, generation: int) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"director:conversation-feed:{target.channel_id}:{root}:{generation}"))


def _response_data(value: object) -> Mapping[str, Any] | None:
    if isinstance(value, Mapping):
        return value
    data = getattr(value, "data", None)
    return data if isinstance(data, Mapping) else None


def _successful_ts(response: Mapping[str, Any] | None) -> str | None:
    if response is None or response.get("ok") is False:
        return None
    message = response.get("message")
    value = message.get("ts") if isinstance(message, Mapping) else response.get("ts")
    return value if isinstance(value, str) and _is_ts(value) else None


def _valid_target_response(response: Mapping[str, Any] | None, channel_id: str) -> bool:
    if response is None or response.get('ok') is False:
        return False
    channel = response.get('channel')
    value = channel if isinstance(channel, str) else channel.get('id') if isinstance(channel, Mapping) else None
    return value is None or value == channel_id


def _known_rejection(value: object) -> bool:
    response = _response_data(value) or _response_data(getattr(value, 'response', None))
    return response is not None and response.get('ok') is False or isinstance(response, Mapping) and isinstance(response.get('error'), str)


def _rate_limited(value: object) -> bool:
    response = _response_data(value) or _response_data(getattr(value, 'response', None))
    return isinstance(response, Mapping) and response.get('error') == 'ratelimited'


def _retry_after(value: object) -> float:
    headers = getattr(value, 'headers', None) or getattr(getattr(value, 'response', None), 'headers', None)
    raw = None
    if hasattr(headers, 'get'):
        raw = headers.get('Retry-After') or headers.get('retry-after')
    if isinstance(raw, (list, tuple)):
        raw = raw[0] if raw else None
    try:
        delay = float(raw)
        return delay if delay >= 1.0 and delay != float('inf') else 1.0
    except (TypeError, ValueError, OverflowError):
        return 1.0


def _fallback_text(session: Mapping[str, Any]) -> str:
    if session.get('approval_state'):
        return f"{session['emoji']} {session['title']}: {_approval_status(str(session['approval_state']))}"
    return f"{session['emoji']} {session['title']}: {session['latest_preview']}"


_MAX_SECTION_TEXT = 3000
_MAX_BLOCKS = 50


def _proposal_chunks(value: object) -> list[str] | None:
    """Return the complete plain-text proposal or no safe Slack representation."""
    if not isinstance(value, str) or not value:
        return None
    # Card-B title + label + actions + divider leave 46 full sections.
    maximum = (_MAX_BLOCKS - 4) * _MAX_SECTION_TEXT
    if len(value) > maximum:
        return None
    return [value[index:index + _MAX_SECTION_TEXT] for index in range(0, len(value), _MAX_SECTION_TEXT)]


def _approval_status(state: str) -> str:
    return {
        'pending': 'Approval needed · Send this reply',
        'retrying': 'Retrying approved reply…',
        'failed': 'The approved reply did not finish.',
        'expired': 'The one-time approval window expired.',
        'restart_inactive': 'Approval became inactive after receiver restart.',
        'inactive': 'This approval is no longer active.',
    }.get(state, 'Approval is no longer active.')


def _blocks(session: Mapping[str, Any]) -> list[dict[str, object]]:
    title = _escape_mrkdwn(str(session['title']))
    preview = _escape_mrkdwn(str(session['latest_preview']))
    state = session.get('approval_state')
    if not state:
        return [
            {"type": "section", "text": {"type": "mrkdwn", "text": f"*{session['emoji']} {title}*\n\n{preview}"}},
            {"type": "actions", "elements": [{
                "type": "button", "text": {"type": "plain_text", "text": "Open conversation", "emoji": True},
                "style": "primary", "action_id": "director_open_conversation", "url": session["conversation_url"],
            }]},
            {"type": "divider"},
        ]
    chunks = _proposal_chunks(session.get('approval_text'))
    if chunks is None and state == 'pending':
        status = ('This proposed reply is too large to display completely in Slack. '
                  'Use authenticated local review for this exact reply.')
    else:
        status = _approval_status(str(state))
    blocks: list[dict[str, object]] = [
        {"type": "section", "text": {"type": "mrkdwn", "text": f"*{session['emoji']} {title}*\n\n{_escape_mrkdwn(status)}"}},
    ]
    if chunks is not None:
        blocks.append({"type": "context", "elements": [{"type": "plain_text", "text": "Proposed reply", "emoji": False}]})
        blocks.extend({"type": "section", "text": {"type": "plain_text", "text": chunk, "emoji": False}} for chunk in chunks)
    elements: list[dict[str, object]] = []
    if state == 'pending' and chunks is not None and isinstance(session.get('approval_id'), str):
        elements.append({
            "type": "button", "text": {"type": "plain_text", "text": "Approve once", "emoji": True},
            "style": "primary", "action_id": "director_approve_once", "value": session['approval_id'],
        })
    elements.append({
        "type": "button", "text": {"type": "plain_text", "text": "Open conversation", "emoji": True},
        "action_id": "director_open_conversation", "url": session["conversation_url"],
    })
    blocks.extend([{"type": "actions", "elements": elements}, {"type": "divider"}])
    if len(blocks) > _MAX_BLOCKS:
        raise ConversationFeedError('approval card exceeds Slack block limit')
    return blocks


def _escape_mrkdwn(value: str) -> str:
    return value.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')


class ConversationFeedWorker:
    """A rate-bounded reconciler which never runs on the receiver loop thread."""

    def __init__(self, feed: ConversationFeed, on_error=None) -> None:
        self.feed = feed
        self.on_error = on_error
        self._wake = threading.Event()
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="director-conversation-feed", daemon=True)

    def start(self) -> None:
        if self.feed.enabled:
            self._thread.start()

    def wake(self) -> None:
        if self.feed.enabled:
            self._wake.set()

    def close(self) -> None:
        self._stop.set()
        self._wake.set()
        if self.feed.enabled:
            self._thread.join(timeout=5)

    def _run(self) -> None:
        while not self._stop.is_set():
            self._wake.wait(timeout=30)
            self._wake.clear()
            if self._stop.is_set():
                return
            try:
                result = self.feed.reconcile(limit=1)
                if self.on_error:
                    self.on_error('conversation_feed_pending' if self.feed.has_pending_work() and not (result['posted'] or result['deleted']) else '')
            except Exception as error:
                if self.on_error:
                    self.on_error(type(error).__name__)
            # Slack permits modest traffic, but this feed deliberately makes at
            # most one mutation attempt per second and never busy-spins.
            if self.feed.has_pending_work():
                self._stop.wait(1)
                self._wake.set()
