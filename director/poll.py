"""Compact, local-only Director work and health polling."""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
import time
from typing import Any

from .inbox import Checkpoint, InboxStore
from .reminders import ReminderStore
from .responsibilities import Responsibility, ResponsibilityStore


RECEIVER_STALE_SECONDS = 120
RECOVERY_DUE_SECONDS = 15 * 60
MANAGER_RECOVERY_STALE_SECONDS = 20 * 60
CREDENTIAL_BACKOFF_SECONDS = 15 * 60
ROLES = {"manager", "relay"}


@dataclass(frozen=True)
class _Health:
    code: str
    updated_at: float | None = None

    def as_dict(self) -> dict[str, Any]:
        item: dict[str, Any] = {"code": self.code}
        if self.updated_at is not None:
            item["updated_at"] = self.updated_at
        return item


def collect_poll(
    path: str | Path, *, role: str = "manager", now: float | None = None, enabled: bool = True
) -> dict[str, Any]:
    """Return only durable identifiers, timing, state, and counters.

    This function never opens a Slack session, reads credential values, or
    returns source, reminder, or responsibility prose.  ResponsibilityStore
    reaps an expired execution claim as part of its documented ``list``
    operation, which makes that item runnable again without granting a claim.
    """
    if role not in ROLES:
        raise ValueError("role must be manager or relay")
    timestamp = time.time() if now is None else float(now)
    if not enabled:
        return {
            "role": role,
            "outcome": "disabled",
            "at": timestamp,
            "sources": [],
            "responsibilities": [],
            "reminders": [],
            "health": [{"code": "receiver_disabled"}],
            "counters": _counters([], [], [], [_Health("receiver_disabled")], []),
        }

    with InboxStore(path) as inbox:
        pending = inbox.list_pending_relays(include_leased=role == "manager", now=timestamp)
        open_messages = inbox.list_open_messages()
        checkpoints = {
            name: inbox.get_checkpoint(name)
            for name in (
                "receiver.loop",
                "receiver.recovery",
                "receiver.reminder_error",
                "dispatcher.error",
                "receiver.intake_notice_error",
                "receiver.recovery_error",
                "receiver.resurface_error",
                "receiver.card_render_error",
                "receiver.card_nudge_error",
                "receiver.conversation_feed_error",
                "receiver.conversation_feed_queue_error",
                "luna.scan",
                "runtime.credentials",
            )
        }

    pending_by_id = {item.message.id: item for item in pending}
    sources: list[dict[str, Any]] = []
    for message in open_messages:
        pending_source = pending_by_id.get(message.id)
        relayed = pending_source is None or pending_source.receipts.is_relayed(message.revision)
        if role == "relay" and relayed:
            continue
        sources.append(
            {
                "message_id": message.id,
                "revision": message.revision,
                "source_ts": message.source_ts,
                "thread_ts": message.thread_ts,
                "action": "review" if relayed else "relay",
            }
        )

    runnable: list[dict[str, Any]] = []
    if role == "manager":
        with ResponsibilityStore(path) as responsibilities:
            records = responsibilities.list()
            runnable = [_responsibility_reference(item, responsibilities) for item in records if item.state == "runnable"]

    with ReminderStore(path) as reminders:
        due_reminders = [
            {"id": item.id, "due_at": item.due_at, "thread_ts": item.thread_ts}
            for item in reminders.list_due(now=timestamp)
        ]

    health = _health(checkpoints, timestamp, role)
    actionable_health = [item for item in health if item.code != "credentials_backoff"]
    actionable = bool(sources or runnable or due_reminders or actionable_health)
    return {
        "role": role,
        "outcome": "actionable" if actionable else "idle",
        "at": timestamp,
        "sources": sources,
        "responsibilities": runnable,
        "reminders": due_reminders,
        "health": [item.as_dict() for item in health],
        "counters": _counters(sources, runnable, due_reminders, health, actionable_health),
    }


def _counters(
    sources: list[dict[str, Any]],
    runnable: list[dict[str, Any]],
    due_reminders: list[dict[str, Any]],
    health: list[_Health],
    actionable_health: list[_Health] | None = None,
) -> dict[str, int]:
    actionable = health if actionable_health is None else actionable_health
    return {
        "sources": len(sources),
        "sources_relay": sum(item["action"] == "relay" for item in sources),
        "sources_review": sum(item["action"] == "review" for item in sources),
        "responsibilities": len(runnable),
        "responsibilities_expired_claim": sum(item["state"] == "expired_claim" for item in runnable),
        "reminders_due": len(due_reminders),
        "health": len(health),
        "health_actionable": len(actionable),
    }


def _responsibility_reference(item: Responsibility, store: ResponsibilityStore) -> dict[str, Any]:
    history = store.history(item.id)
    expired_claim = bool(history and history[-1].event == "claim_expired")
    return {
        "id": item.id,
        "state": "expired_claim" if expired_claim else item.state,
        "deadline_at": item.deadline_at,
        "version": item.version,
    }


def _health(checkpoints: dict[str, Checkpoint | None], now: float, role: str) -> list[_Health]:
    result: list[_Health] = []
    loop = checkpoints["receiver.loop"]
    if loop is None:
        result.append(_Health("receiver_unseen"))
    else:
        try:
            connected = json.loads(loop.value)["connected"]
            if type(connected) is not bool:
                raise ValueError
        except (json.JSONDecodeError, KeyError, TypeError, ValueError):
            result.append(_Health("receiver_state_invalid", loop.updated_at))
        else:
            if not connected:
                result.append(_Health("receiver_disconnected", loop.updated_at))
        if now - loop.updated_at > RECEIVER_STALE_SECONDS:
            result.append(_Health("receiver_stale", loop.updated_at))

    credentials = checkpoints["runtime.credentials"]
    credential_backoff = False
    if credentials is not None and credentials.value == "unavailable":
        credential_backoff = now - credentials.updated_at < CREDENTIAL_BACKOFF_SECONDS
        code = "credentials_backoff" if credential_backoff else "credentials_recovery_due"
        result.append(_Health(code, credentials.updated_at))

    recoveries = [item for item in (checkpoints["luna.scan"], checkpoints["receiver.recovery"]) if item is not None]
    recovery = max(recoveries, key=lambda item: item.updated_at) if recoveries else None
    recovery_age = None if recovery is None else now - recovery.updated_at
    if not credential_backoff:
        if role == "relay" and (recovery_age is None or recovery_age > RECOVERY_DUE_SECONDS):
            result.append(_Health("recovery_due", None if recovery is None else recovery.updated_at))
        elif role == "manager" and (recovery_age is None or recovery_age > MANAGER_RECOVERY_STALE_SECONDS):
            result.append(_Health("recovery_stale", None if recovery is None else recovery.updated_at))

    for name, code in (
        ("receiver.recovery_error", "recovery_error"),
        ("receiver.reminder_error", "reminder_error"),
        ("dispatcher.error", "dispatcher_error"),
        ("receiver.intake_notice_error", "intake_notice_error"),
        ("receiver.resurface_error", "resurface_error"),
        ("receiver.card_render_error", "card_render_error"),
        ("receiver.card_nudge_error", "card_nudge_error"),
        ("receiver.conversation_feed_error", "conversation_feed_error"),
        ("receiver.conversation_feed_queue_error", "conversation_feed_queue_error"),
    ):
        checkpoint = checkpoints[name]
        if checkpoint is not None and checkpoint.value:
            result.append(_Health(code, checkpoint.updated_at))
    return result
